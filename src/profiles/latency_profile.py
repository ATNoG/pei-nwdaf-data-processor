from typing import override
from statistics import mean, stdev
from src.profiles.processing_profile import ProcessingProfile
from src.empty_window_strategy import EmptyWindowStrategy
from typing import Any


class LatencyProfile(ProcessingProfile):
    FIELDS = ["rsrp", "sinr", "rsrq", "mean_latency", "cqi"]
    TIME_FIELD = "timestamp"

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

        network = data[0].get("network")
        primary_bandwidth = data[0].get("primary_bandwidth")
        ul_bandwidth = data[0].get("ul_bandwidth")


        if not all(d.get("cell_index") == first_cell_index for d in data):
            return None

        timestamps = [
            d.get(cls.TIME_FIELD) for d in data
            if d.get(cls.TIME_FIELD) is not None
        ]

        start_time =    min(timestamps) if timestamps else None
        end_time =      max(timestamps) if timestamps else None


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
            "type":"latency",
            "cell_index": first_cell_index,
            "network":network,
            "sample_count": total_samples,
            "primary_bandwidth":primary_bandwidth,
            "ul_bandwidth":ul_bandwidth,
            **stats
        }

    @classmethod
    @override
    def handle_empty_window(cls, cell_id: str, window_start: int, window_end: int, strategy: EmptyWindowStrategy) -> dict | None:
        """Handle empty window for latency profile."""

        #TODO: Implement other strategies, such as ZERO_FILL or FORWARD_FILL

        # Create base structure
        #empty_stats = {
        #    field: {
        #        "min": 0.0,
        #        "max": 0.0,
        #        "mean": 0.0,
        #        "std": 0.0,
        #        "samples": 0
        #    }
        #    for field in cls.FIELDS
        #}
        #
        #
        if strategy == EmptyWindowStrategy.SKIP:
            return None

        return {
            "cell_index": cell_id,
            "sample_count": 0,
        }
