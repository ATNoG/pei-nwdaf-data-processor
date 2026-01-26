from typing import Any, override
from statistics import mean, stdev
from src.profiles.processing_profile import ProcessingProfile
from src.empty_window_strategy import EmptyWindowStrategy

class LatencyProfile(ProcessingProfile):
    FIELDS = ["rsrp", "sinr", "rsrq", "mean_latency", "cqi"]
    TIME_FIELD = "timestamp"
    # Added physical_cellid and server_ip for KNN matching if available
    METADATA_FIELDS = ["network", "primary_bandwidth", "ul_bandwidth", "physical_cellid", "server_ip"]

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
        physical_cellid = data[0].get("physical_cellid")
        server_ip = data[0].get("server_ip")

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
            "type": "latency",
            "cell_index": first_cell_index,
            "network": network,
            "sample_count": total_samples,
            "primary_bandwidth": primary_bandwidth,
            "ul_bandwidth": ul_bandwidth,
            "physical_cellid": physical_cellid,
            "server_ip": server_ip,
            **stats
        }

    @classmethod
    @override
    def get_empty_window_context(cls, cell_id: str, last_processed: dict | None = None) -> dict[str, Any]:
        """Provide latency profile specific context for empty window handling."""
        context = {
            'fields': cls.FIELDS,
            'metadata': {}
        }

        # If we have last processed data, include it for strategies that need it
        if last_processed:
            context['last_values'] = last_processed

            # Extract metadata from last processed if available
            for field in cls.METADATA_FIELDS:
                if field in last_processed:
                    context['metadata'][field] = last_processed[field]

            # For KNN: Extract current network state from the last processed window
            # This represents the most recent known network conditions
            network_state = {}
            for field in ['rsrp', 'sinr', 'rsrq', 'rssi']:
                if field in last_processed and isinstance(last_processed[field], dict):
                    # Use the mean as the current state indicator
                    mean_val = last_processed[field].get('mean')
                    if mean_val is not None:
                        network_state[field] = mean_val

            # Add physical_cellid and server_ip if available (for categorical matching)
            if 'physical_cellid' in last_processed and last_processed['physical_cellid'] is not None:
                network_state['physical_cellid'] = last_processed['physical_cellid']
            if 'server_ip' in last_processed and last_processed['server_ip'] is not None:
                network_state['server_ip'] = last_processed['server_ip']

            # Add network state to context for KNN strategy
            if network_state:
                context['current_network_state'] = network_state

        return context
