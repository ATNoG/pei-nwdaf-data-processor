from typing import override, Optional, Dict, Any
from statistics import mean, stdev
from src.profiles.processing_profile import ProcessingProfile
from src.empty_window_strategy import EmptyWindowStrategy

class LatencyProfile(ProcessingProfile):
    FIELDS = ["rsrp", "sinr", "rsrq", "mean_latency", "cqi"]
    TIME_FIELD = "timestamp"
    METADATA_FIELDS = ["network", "primary_bandwidth", "ul_bandwidth"]

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
    def get_empty_window_context(cls, cell_id: str, last_processed: Optional[Dict] = None) -> Dict[str, Any]:
        """Provide latency profile specific context for empty window handling."""
        context = {
            'fields': cls.FIELDS,
            'metadata': {}  # Could be populated from configuration or cell metadata store
        }
        
        # If we have last processed data, include it for forward-fill strategy
        if last_processed:
            context['last_values'] = last_processed
            # Extract metadata from last processed if available
            for field in cls.METADATA_FIELDS:
                if field in last_processed:
                    context['metadata'][field] = last_processed[field]
        
        return context

        
