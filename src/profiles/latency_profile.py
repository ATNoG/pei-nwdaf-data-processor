from typing import override
from statistics import mean, stdev
from src.profiles.processing_profile import ProcessingProfile
from typing import Any


class LatencyProfile(ProcessingProfile):
    FIELDS = ["rsrp", "sinr", "rsrq", "latency", "cqi"]

    @classmethod
    @override
    def process(cls, data: list[dict]) -> dict | None:
        """
        Aggregates latency-related metrics ONLY if all samples are from the same cell.

        Args:
            data: List of metric dictionaries, each must contain 'cell_index' and the fields above.

        Returns:
            Dict with statistics per field + metadata if all from same cell_index,
            otherwise None.
        """
        if not data:
            return None

        first_cell_index = data[0].get("cell_index")
        if first_cell_index is None:
            return None

        if not all(d.get("cell_index") == first_cell_index for d in data):
            return None

        # keep numeric fields only
        values: dict[str, list[float]] = {field: [] for field in cls.FIELDS}
        for entry in data:
            for field in cls.FIELDS:
                val = entry.get(field)
                if isinstance(val, (int, float)) and val is not None:
                    values[field].append(float(val))

        # compute stats
        stats: dict[str, Any] = {}
        total_samples = len(data)

        for field in cls.FIELDS:
            field_vals = values[field]
            count = len(field_vals)

            if count == 0:
                stats[field] = {
                    "min": None,
                    "max": None,
                    "mean": None,
                    "std": None,
                    "samples": 0
                }
            else:
                stats[field] = {
                    "min": min(field_vals),
                    "max": max(field_vals),
                    "mean": mean(field_vals),
                    "std": stdev(field_vals) if count > 1 else 0.0,
                    "samples": count
                }

        return {
            "cell_index": first_cell_index,
            "num_samples": total_samples,
            "stats": stats
        }
