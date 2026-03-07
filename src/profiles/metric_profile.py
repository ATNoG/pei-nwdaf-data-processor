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
