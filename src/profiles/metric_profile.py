import base64
import logging
from collections import defaultdict
from statistics import mean, stdev
from typing import Any, override

import tenseal as ts

from src.profiles.processing_profile import ProcessingProfile

logger = logging.getLogger(__name__)

# Fields that are never aggregated as numeric metrics.
_NON_METRIC_FIELDS = {"timestamp", "cell_index", "ip_src"}

# Metadata fields carried through as-is (first value in group wins).
_METADATA_FIELDS = ("network", "primary_bandwidth", "ul_bandwidth", "physical_cellid", "server_ip")

# Wire-format keys injected by HomomorphicEncryptionTransformer.
_FHE_META_KEYS = {"__fhe_context__", "__fhe_encrypted_fields__"}


def _load_public_context(b64: str) -> ts.Context | None:
    try:
        return ts.context_from(base64.b64decode(b64))
    except Exception as exc:
        logger.warning("Failed to load FHE public context: %s", exc)
        return None


def _homomorphic_mean(vectors: list[ts.CKKSVector]) -> ts.CKKSVector:
    """Compute encrypted mean via homomorphic addition + plaintext scalar multiply."""
    enc_sum = vectors[0]
    for v in vectors[1:]:
        enc_sum = enc_sum + v
    return enc_sum * (1.0 / len(vectors))


class MetricProfile(ProcessingProfile):
    TIME_FIELD = "timestamp"
    IP_FIELD = "ip_src"

    @classmethod
    @override
    def process(cls, data: list[dict]) -> list[dict]:
        if not data:
            return []

        results = []

        # Always aggregate by cell_index (all records)
        cell_groups: dict[Any, list[dict]] = defaultdict(list)
        for record in data:
            cell = record.get("cell_index")
            if cell is not None:
                cell_groups[cell].append(record)

        for cell, group_data in cell_groups.items():
            result = cls._aggregate_group(("cell_index",), group_data)
            if result is not None:
                results.append(result)

        # Additionally aggregate by (cell_index, ip_src) for records that have ip_src
        ip_groups: dict[tuple, list[dict]] = defaultdict(list)
        for record in data:
            cell = record.get("cell_index")
            ip = record.get(cls.IP_FIELD)
            if cell is not None and ip is not None:
                ip_groups[(cell, ip)].append(record)

        for group_data in ip_groups.values():
            result = cls._aggregate_group(("cell_index", cls.IP_FIELD), group_data)
            if result is not None:
                results.append(result)

        return results

    @classmethod
    def _aggregate_group(cls, key_fields: tuple[str, ...], data: list[dict]) -> dict | None:
        if not data:
            return None

        first = data[0]

        # Detect FHE-encrypted fields and load public context for homomorphic ops
        encrypted_fields: set[str] = set(first.get("__fhe_encrypted_fields__") or [])
        ckks_ctx: ts.Context | None = None
        if encrypted_fields:
            ctx_b64 = first.get("__fhe_context__")
            if ctx_b64:
                ckks_ctx = _load_public_context(ctx_b64)
            if ckks_ctx is None:
                logger.warning(
                    "FHE-encrypted fields %s found but context unavailable — treating as opaque",
                    encrypted_fields,
                )
                encrypted_fields = set()

        # Collect all numeric values per field (plaintext)
        values: dict[str, list[float]] = defaultdict(list)
        # Collect CKKS ciphertext vectors per field (encrypted)
        enc_vectors: dict[str, list[ts.CKKSVector]] = defaultdict(list)

        for entry in data:
            for field, val in entry.items():
                if field in _NON_METRIC_FIELDS or field in _FHE_META_KEYS:
                    continue

                if field in encrypted_fields and ckks_ctx is not None:
                    blob = val
                    if isinstance(blob, dict) and blob.get("__fhe__"):
                        try:
                            raw = base64.b64decode(blob["ciphertext"])
                            enc = ts.ckks_vector_from(ckks_ctx, raw)
                            enc_vectors[field].append(enc)
                        except Exception as exc:
                            logger.warning("Could not deserialize FHE vector for %r: %s", field, exc)
                elif isinstance(val, (int, float)):
                    values[field].append(float(val))

        # Compute stats for plaintext fields
        stats: dict[str, Any] = {}
        for field, field_vals in values.items():
            count = len(field_vals)
            if count == 0:
                stats[field] = {"min": None, "max": None, "mean": None, "std": None, "samples": 0}
            else:
                stats[field] = {
                    "min": min(field_vals),
                    "max": max(field_vals),
                    "mean": mean(field_vals),
                    "std": stdev(field_vals) if count > 1 else 0.0,
                    "samples": count,
                }

        # Collect original_type tags from the first blob per field so decryption
        # can restore the original Python type (e.g. IPv4 string from "ipv4" tag).
        original_types: dict[str, str] = {}
        for entry in data:
            for field in encrypted_fields:
                blob = entry.get(field)
                if isinstance(blob, dict) and "original_type" in blob and field not in original_types:
                    original_types[field] = blob["original_type"]

        # Compute homomorphic mean for CKKS-encrypted fields
        # min/max/std are unavailable without decryption
        for field, vecs in enc_vectors.items():
            if not vecs:
                continue
            try:
                enc_mean = _homomorphic_mean(vecs)
                stats[field] = {
                    "mean": base64.b64encode(enc_mean.serialize()).decode(),
                    "min": None,
                    "max": None,
                    "std": None,
                    "samples": len(vecs),
                    "__fhe__": True,
                    "scheme": "CKKS",
                    **({"original_type": original_types[field]} if field in original_types else {}),
                }
            except Exception as exc:
                logger.warning("Homomorphic mean failed for %r: %s — dropping field", field, exc)

        # Build result with group key fields
        first = data[0]
        result: dict[str, Any] = {"type": "metric", "sample_count": len(data)}

        # Add grouping keys
        for f in key_fields:
            result[f] = first[f]

        # Add metadata from first record
        for field in _METADATA_FIELDS:
            val = first.get(field)
            if val is not None:
                result[field] = val

        result.update(stats)

        # Carry FHE metadata through so authorized downstream consumers can decrypt
        remaining_encrypted = [f for f in encrypted_fields if f in enc_vectors]
        if remaining_encrypted:
            result["__fhe_context__"] = first.get("__fhe_context__")
            result["__fhe_encrypted_fields__"] = sorted(remaining_encrypted)

        return result

    @classmethod
    @override
    def get_empty_window_context(
        cls, cell_id: str, last_processed: dict | None = None
    ) -> dict[str, Any]:
        context: dict[str, Any] = {"metadata": {}}

        if last_processed:
            context["last_values"] = last_processed

            for field in _METADATA_FIELDS:
                if field in last_processed:
                    context["metadata"][field] = last_processed[field]

            # Collect plaintext field names from last processed (skip FHE-encrypted fields)
            fields = [
                k for k, v in last_processed.items()
                if isinstance(v, dict) and "mean" in v and not v.get("__fhe__")
            ]
            context["fields"] = fields

            # Network state for strategies that need it
            network_state = {}
            for field in fields:
                if isinstance(last_processed[field], dict):
                    mean_val = last_processed[field].get("mean")
                    if mean_val is not None:
                        network_state[field] = mean_val

            for field in ("physical_cellid", "server_ip"):
                if field in last_processed and last_processed[field] is not None:
                    network_state[field] = last_processed[field]

            if network_state:
                context["current_network_state"] = network_state
        else:
            context["fields"] = []

        return context
