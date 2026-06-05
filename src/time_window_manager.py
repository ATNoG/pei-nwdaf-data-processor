import logging
from collections import defaultdict
from datetime import datetime
from statistics import mean, stdev
from typing import Callable

from cachetools import TTLCache

from src.empty_window_strategy import EmptyWindowStrategy, SkipStrategy
from src.window_buffer import CellWindowBuffer

logger = logging.getLogger("TimeWindowManager")


class TimeWindowManager:
    """
    Manages event-time aligned windows for measurements grouped by network slice.
    Window lifecycle is driven by watermark advancement.
    Records are ingested directly via ingest() — no external HTTP fetch.

    Group key: (snssai_sst, snssai_sd, dnn, event)
    Records missing snssai_sst or dnn are dropped with a warning.

    Sliding vs tumbling: set slide_interval < window_size for sliding windows.
    When slide_interval == window_size (default), behaviour is identical to tumbling windows.

    During warm-up (watermark_time < window_size), window_start is clamped to 0 so
    partial windows are still emitted. This is intentional.

    TTL pruning: if group_ttl is set, groups that receive no data for group_ttl
    wall-clock seconds are pruned after their last window is emitted.
    Uses cachetools.TTLCache as a heartbeat — each ingest() resets the TTL via
    __setitem__. Pruning fires after window emission to avoid dropping the final window.
    """

    def __init__(
        self,
        window_size: int,
        on_window_complete: Callable | None = None,
        empty_window_strategy: EmptyWindowStrategy | None = None,
        slide_interval: int | None = None,
        group_ttl: int | None = None,
    ):
        self.window_size = window_size
        self.slide_interval = slide_interval if slide_interval is not None else window_size

        if self.slide_interval <= 0 or self.slide_interval > self.window_size:
            raise ValueError(
                f"slide_interval must be in (0, window_size]. "
                f"Got slide_interval={self.slide_interval}, window_size={self.window_size}"
            )

        self.on_window_complete = on_window_complete or print
        self.empty_window_strategy = empty_window_strategy or SkipStrategy()
        self.group_ttl = group_ttl

        self.watermark: int = 0
        self._can_change_watermark = True
        self._last_processed: dict[tuple, dict] = {}
        self._buffers: dict[tuple, CellWindowBuffer] = {}
        # Heartbeat: key present ↔ group received data within group_ttl wall-clock seconds.
        # TTLCache auto-expires entries; __setitem__ in ingest() resets the clock.
        self._active: TTLCache | None = (
            TTLCache(maxsize=10_000, ttl=group_ttl) if group_ttl is not None else None
        )

    def _group_key(self, record: dict) -> tuple | None:
        tags = record.get("tags", {})
        sst = tags.get("snssai_sst")
        dnn = tags.get("dnn")
        if sst is None or dnn is None:
            logger.warning(f"Record missing snssai_sst or dnn, skipping: tags={tags}")
            return None
        sd = tags.get("snssai_sd", "")
        event = record.get("event", "")
        return (str(sst), str(sd), str(dnn), str(event))

    def _key_to_tags(self, key: tuple) -> dict:
        sst, sd, dnn, event = key
        return {"snssai_sst": sst, "snssai_sd": sd, "dnn": dnn, "event": event}

    def ingest(self, record: dict) -> None:
        key = self._group_key(record)
        if key is None:
            return
        ts = record.get("timestamp")
        if isinstance(ts, str):
            record = dict(record)
            record["timestamp"] = int(datetime.fromisoformat(ts).timestamp())
        if key not in self._buffers:
            self._buffers[key] = CellWindowBuffer(time_field="timestamp")
        self._buffers[key].append_slice([record])
        if self._active is not None:
            self._active[key] = True  # resets wall-clock TTL

    def set_initial_watermark(self, watermark: int) -> None:
        if self._can_change_watermark:
            self.watermark = watermark
            self._can_change_watermark = False

    async def advance_watermark(self, watermark_time: int) -> None:
        # advance_watermark has no internal awaits — it is async so callers can
        # await it consistently, and to preserve the option to add async I/O later.
        # ingest() and advance_watermark() are both called from the asyncio event
        # loop (ingest via the result of run_in_executor, not inside it), so there
        # is no concurrent access to _buffers.
        if watermark_time <= self.watermark:
            logger.error(f"Cannot move watermark backwards: {self.watermark} > {watermark_time}")
            return
        self._can_change_watermark = False

        # window_start is clamped to 0 during warm-up (watermark_time < window_size).
        window_start = max(0, watermark_time - self.window_size)
        window_end = watermark_time

        for key, buf in list(self._buffers.items()):
            buf.evict_before(window_start)
            window_data = buf.get_all()

            if not window_data:
                last = self._last_processed.get(key)
                context = {
                    "tags": self._key_to_tags(key),
                    "fields": list(last["metrics"].keys()) if last else [],
                    "window_duration_seconds": self.window_size,
                }
                result = self.empty_window_strategy.handle(str(key), window_start, window_end, context)
                if result is not None:
                    self.on_window_complete(result)
            else:
                result = self._aggregate(key, window_data, window_start, window_end)
                self._last_processed[key] = result
                self.on_window_complete(result)

        # Prune after emission so the last window of a dying group is not lost.
        self._prune_stale_groups(watermark_time)

        self.watermark = watermark_time

    def _prune_stale_groups(self, watermark_time: int) -> None:
        if self._active is None:
            return
        stale = [k for k in list(self._buffers) if k not in self._active]
        for k in stale:
            self._buffers.pop(k)
            self._last_processed.pop(k, None)
            logger.info(f"Pruned stale group {k} at watermark={watermark_time}")

    def _aggregate(self, key: tuple, data: list[dict], window_start: int, window_end: int) -> dict:
        import base64
        try:
            import tenseal as ts
            _TENSEAL = True
        except ImportError:
            _TENSEAL = False

        first = data[0]
        encrypted_fields: set[str] = set(first.get("__fhe_encrypted_fields__") or [])
        ckks_ctx = None
        if encrypted_fields and _TENSEAL:
            ctx_b64 = first.get("__fhe_context__")
            if ctx_b64:
                try:
                    ckks_ctx = ts.context_from(base64.b64decode(ctx_b64))
                except Exception as e:
                    logger.warning("Failed to load FHE context: %s", e)

        plain_values: dict[str, list[float]] = defaultdict(list)
        enc_vectors: dict[str, list] = defaultdict(list)

        for record in data:
            metrics = record.get("metrics", {})
            for field, val in metrics.items():
                if field in encrypted_fields and ckks_ctx is not None:
                    if isinstance(val, dict) and val.get("__fhe__"):
                        try:
                            raw = base64.b64decode(val["ciphertext"])
                            vec = ts.ckks_vector_from(ckks_ctx, raw)
                            enc_vectors[field].append(vec)
                        except Exception as e:
                            logger.warning("Could not deserialize FHE vector for %r: %s", field, e)
                elif isinstance(val, (int, float)):
                    plain_values[field].append(float(val))

        # std uses sample standard deviation (N-1). Downstream ML consumers
        # expecting population std should divide by sqrt(count/(count-1)).
        stats: dict = {}
        for field, field_vals in plain_values.items():
            count = len(field_vals)
            stats[field] = {
                "mean": mean(field_vals),
                "min": min(field_vals),
                "max": max(field_vals),
                "std": stdev(field_vals) if count > 1 else 0.0,
                "count": count,
            }

        enc_aggregated: list[str] = []
        for field, vecs in enc_vectors.items():
            if not vecs:
                continue
            try:
                enc_sum = vecs[0]
                for v in vecs[1:]:
                    enc_sum = enc_sum + v
                enc_mean = enc_sum * (1.0 / len(vecs))
                stats[field] = {
                    "__fhe__": True,
                    "ciphertext": base64.b64encode(enc_mean.serialize()).decode(),
                    "scheme": "CKKS",
                    "samples": len(vecs),
                    "min": None, "max": None, "std": None,
                }
                enc_aggregated.append(field)
            except Exception as e:
                logger.warning("Homomorphic mean failed for %r: %s — dropping field", field, e)

        result: dict = {
            "tags": self._key_to_tags(key),
            "window_start": window_start,
            "window_end": window_end,
            "window_duration_seconds": self.window_size,
            "sample_count": len(data),
            "metrics": stats,
        }
        if enc_aggregated:
            result["__fhe_context__"] = first.get("__fhe_context__")
            result["__fhe_encrypted_fields__"] = sorted(enc_aggregated)

        return result
