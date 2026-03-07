from collections import defaultdict
from statistics import mean, stdev
from typing import Any, override

from src.profiles.processing_profile import ProcessingProfile


# Fields that are never aggregated as numeric metrics.
_NON_METRIC_FIELDS = {"timestamp", "cell_index", "ip_src"}

# Metadata fields carried through as-is (first value in group wins).
_METADATA_FIELDS = ("network", "primary_bandwidth", "ul_bandwidth", "physical_cellid", "server_ip")


class MetricProfile(ProcessingProfile):
    TIME_FIELD = "timestamp"
    PRIORITY = [("cell_index", "ip_src"), ("cell_index",)]

    @classmethod
    @override
    def process(cls, data: list[dict]) -> list[dict]:
        if not data:
            return []

        used_key_fields, groups = cls._group_by_priority(data)
        results = []
        for group_data in groups.values():
            result = cls._aggregate_group(used_key_fields, group_data)
            if result is not None:
                results.append(result)
        return results

    @classmethod
    def _group_by_priority(cls, data: list[dict]) -> tuple[tuple[str, ...], dict[tuple, list[dict]]]:
        """Group records by the best available key from PRIORITY.
        Returns the key fields used and the grouped data."""
        for key_fields in cls.PRIORITY:
            groups: dict[tuple, list[dict]] = defaultdict(list)
            ungroupable = False
            for record in data:
                key_values = tuple(record.get(f) for f in key_fields)
                if any(v is None for v in key_values):
                    ungroupable = True
                    break
                groups[key_values].append(record)
            if not ungroupable:
                return key_fields, dict(groups)
        # Fallback: cannot group at all
        return (), {}

    @classmethod
    def _aggregate_group(cls, key_fields: tuple[str, ...], data: list[dict]) -> dict | None:
        if not data:
            return None

        # Collect all numeric values per field
        values: dict[str, list[float]] = defaultdict(list)
        for entry in data:
            for field, val in entry.items():
                if field in _NON_METRIC_FIELDS:
                    continue
                if isinstance(val, (int, float)):
                    values[field].append(float(val))

        # Compute stats
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

            # Collect field names from last processed for strategies that need them
            fields = [
                k for k, v in last_processed.items()
                if isinstance(v, dict) and "mean" in v
            ]
            context["fields"] = fields

            # Network state for KNN
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
