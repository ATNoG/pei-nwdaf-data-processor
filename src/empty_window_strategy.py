from abc import ABC, abstractmethod
from typing import Optional, Dict, Any, List, Tuple
import numpy as np
from datetime import datetime
from collections import defaultdict


class EmptyWindowStrategy(ABC):
    """Base strategy for handling empty windows"""

    @abstractmethod
    def handle(self, cell_id: str, window_start: int, window_end: int,
               context: Optional[Dict[str, Any]] = None) -> Optional[Dict]:
        """
        Handle an empty window.

        Args:
            cell_id: Identifier for the cell
            window_start: Start timestamp of the window
            window_end: End timestamp of the window
            context: Optional context data (e.g., profile-specific fields, last known values)

        Returns:
            Dictionary with processed data for the empty window, or None to skip
        """
        raise NotImplementedError


class SkipStrategy(EmptyWindowStrategy):
    """Skip empty windows - don't generate any output"""

    def handle(self, cell_id: str, window_start: int, window_end: int,
               context: Optional[Dict[str, Any]] = None) -> Optional[Dict]:
        """Return None to skip processing this window"""
        return None


class ZeroFillStrategy(EmptyWindowStrategy):
    """Fill empty windows with zero/null values"""

    def handle(self, cell_id: str, window_start: int, window_end: int,
               context: Optional[Dict[str, Any]] = None) -> Optional[Dict]:
        """
        Generate a record with zero/null values for all metrics.

        Context should contain:
            - fields: List of field names to zero-fill
            - metadata: Dict of non-metric metadata to include (network, bandwidth, etc.)
        """
        if not context or 'fields' not in context:
            return None

        result = {
            'cell_index': cell_id,
            'sample_count': 0,
            'start_time': window_start,
            'end_time': window_end,
            'is_empty_window': True
        }

        # Add metadata if provided
        if 'metadata' in context:
            result.update(context['metadata'])

        # Zero-fill all metric fields
        for field in context['fields']:
            result[field] = {
                'min': None,
                'max': None,
                'mean': None,
                'std': None,
                'samples': 0
            }

        return result


class ForwardFillStrategy(EmptyWindowStrategy):
    """Forward-fill empty windows with the last known values"""

    def handle(self, cell_id: str, window_start: int, window_end: int,
               context: Optional[Dict[str, Any]] = None) -> Optional[Dict]:
        """
        Generate a record using the last known values.

        Context should contain:
            - last_values: Dict with the last known processed values
        """
        if not context or 'last_values' not in context:
            # If no previous values, fall back to skip
            return None

        last_values = context['last_values'].copy()

        # Update timestamps to reflect the current window
        last_values['start_time'] = window_start
        last_values['end_time'] = window_end
        last_values['sample_count'] = 0
        last_values['is_empty_window'] = True
        last_values['forward_filled'] = True

        return last_values

class KNNStrategy(EmptyWindowStrategy):
    """Fill empty windows using K-Nearest Neighbors based on network context"""

    def __init__(self, k: int = 5, max_history_hours: int = 168,
                 history_buffer: Optional[Dict[str, List[Dict]]] = None):
        """
        Initialize KNN strategy.

        Args:
            k: Number of nearest neighbors to use
            max_history_hours: Maximum hours of history to search (default: 1 week)
            history_buffer: External history buffer (dict[cell_id -> list of windows])
                           If None, creates internal buffer (not recommended for production)
        """
        self.k = k
        self.max_history_seconds = max_history_hours * 3600
        # Use external buffer if provided, otherwise create internal
        self.history_buffer = history_buffer if history_buffer is not None else defaultdict(list)
        self._owns_buffer = history_buffer is None

    def add_to_history(self, cell_id: str, window_data: Dict[str, Any]):
        """
        Add a processed window to the history buffer for future KNN lookups.
        Call this for every non-empty window processed.

        Note: Typically called by TimeWindowManager after processing each window.

        Args:
            cell_id: Cell identifier
            window_data: The processed window data with all metrics
        """
        if not window_data or window_data.get('is_empty_window', False):
            return  # Don't add empty windows to history

        # Convert cell_id to string for consistent keying
        cell_key = str(cell_id)

        if cell_key not in self.history_buffer:
            self.history_buffer[cell_key] = []

        self.history_buffer[cell_key].append(window_data)

        # Prune old history to manage memory
        cutoff_time = window_data.get('window_end', 0) - self.max_history_seconds
        self.history_buffer[cell_key] = [
            w for w in self.history_buffer[cell_key]
            if w.get('window_end', 0) > cutoff_time
        ]

    def handle(self, cell_id: str, window_start: int, window_end: int,
               context: Optional[Dict[str, Any]] = None) -> Optional[Dict]:
        """
        Fill empty window using KNN based on network conditions and temporal features.

        Context should contain:
            - fields: List of field names to fill
            - metadata: Dict of non-metric metadata (network, cell_id, etc.)
            - current_network_state: Dict with current network metrics:
                - rsrp, rssi, sinr, rsrq (signal quality)
                - physical_cellid (optional, for multi-cell scenarios)
                - server_ip (optional, for server-specific patterns)
        """
        if not context:
            return None

        # Extract features for similarity matching
        features = self._extract_features(window_start, context)

        # Find K nearest neighbors from history
        candidates = self._get_candidates(cell_id, window_start)
        if len(candidates) < 1:
            # Fallback to forward fill if no history available
            return self._fallback_fill(cell_id, window_start, window_end, context)

        neighbors = self._find_knn(features, candidates, context)
        if not neighbors:
            return self._fallback_fill(cell_id, window_start, window_end, context)

        # Generate filled window by averaging neighbors
        filled_window = self._aggregate_neighbors(
            neighbors, cell_id, window_start, window_end, context
        )

        return filled_window

    def _extract_features(self, timestamp: int, context: Dict[str, Any]) -> Dict[str, Any]:
        """Extract features for similarity matching"""
        dt = datetime.fromtimestamp(timestamp)

        features = {
            'hour': dt.hour,
            'day_of_week': dt.weekday(),
            'is_weekend': dt.weekday() >= 5,
        }

        # Add network state features if available
        net_state = context.get('current_network_state', {})
        for key in ['rsrp', 'rssi', 'sinr', 'rsrq', 'physical_cellid', 'server_ip']:
            if key in net_state and net_state[key] is not None:
                features[key] = net_state[key]

        # Add previous window metrics if available
        if 'last_values' in context:
            last = context['last_values']
            for field in ['mean_latency', 'packet_loss', 'rsrp', 'sinr']:
                if field in last and isinstance(last[field], dict):
                    features[f'prev_{field}'] = last[field].get('mean')

        return features

    def _get_candidates(self, cell_id: str, current_time: int) -> List[Dict]:
        """Get candidate windows from history (only from the past)"""
        candidates = []

        # Convert to string for consistent keying
        cell_key = str(cell_id)

        # Primary: Same cell_id
        for window in self.history_buffer.get(cell_key, []):
            if window.get('window_end', 0) < current_time:
                candidates.append(window)

        # If not enough candidates, could expand to nearby cells/similar conditions
        # For now, keeping it simple with same cell only

        return candidates

    def _find_knn(self, features: Dict[str, Any], candidates: List[Dict],
                  context: Dict[str, Any]) -> List[Tuple[Dict, float]]:
        """Find K nearest neighbors based on feature distance"""
        distances = []

        for candidate in candidates:
            distance = self._calculate_distance(features, candidate, context)
            distances.append((candidate, distance))

        # Sort by distance and take top K
        distances.sort(key=lambda x: x[1])
        return distances[:self.k]

    def _calculate_distance(self, features: Dict[str, Any],
                           candidate: Dict[str, Any],
                           context: Dict[str, Any]) -> float:
        """
        Calculate weighted distance between current features and candidate window.
        Lower distance = more similar.
        """
        distance = 0.0
        weights = {
            'hour': 2.0,              # Temporal similarity is important
            'day_of_week': 1.0,
            'rsrp': 5.0,              # Signal strength very important
            'sinr': 5.0,              # Signal quality very important
            'rssi': 3.0,
            'rsrq': 3.0,
            'physical_cellid': 10.0,  # Same physical cell = highly similar
            'server_ip': 2.0,         # Same server target
            'prev_mean_latency': 3.0, # Recent history context
            'prev_rsrp': 4.0,
            'prev_sinr': 4.0,
        }

        for key, weight in weights.items():
            if key not in features:
                continue

            feature_val = features[key]

            # Extract candidate value from appropriate location
            if key.startswith('prev_'):
                field = key.replace('prev_', '')
                candidate_val = candidate.get(field, {})
                if isinstance(candidate_val, dict):
                    candidate_val = candidate_val.get('mean')
            else:
                # Direct access - candidate windows have flat structure
                # Try to get the field directly (for metadata fields like physical_cellid, server_ip)
                candidate_val = candidate.get(key)
                # If it's a metric field with stats, get the mean
                if isinstance(candidate_val, dict):
                    candidate_val = candidate_val.get('mean')

            if candidate_val is None:
                continue

            # Calculate component distance based on type
            if key == 'physical_cellid' or key == 'server_ip':
                # Categorical: 0 if match, 1 if not
                component_dist = 0.0 if feature_val == candidate_val else 1.0
            elif key == 'hour':
                # Circular distance for hour (0-23)
                diff = abs(feature_val - candidate_val)
                component_dist = min(diff, 24 - diff) / 12.0  # Normalize to [0, 1]
            elif key == 'day_of_week':
                # Treat weekend vs weekday as more important than specific day
                is_weekend_feature = feature_val >= 5
                is_weekend_candidate = candidate_val >= 5
                component_dist = 0.0 if is_weekend_feature == is_weekend_candidate else 0.5
            else:
                # Numeric: normalized absolute difference
                # For RSRP/SINR, assume typical ranges
                if key in ['rsrp', 'prev_rsrp']:
                    range_val = 50  # RSRP typically ranges ~50 dBm
                elif key in ['sinr', 'prev_sinr']:
                    range_val = 30  # SINR typically ranges ~30 dB
                elif key in ['rssi']:
                    range_val = 50
                elif key in ['rsrq']:
                    range_val = 20
                elif key in ['prev_mean_latency']:
                    range_val = 100  # Latency can range widely
                else:
                    range_val = max(abs(feature_val), abs(candidate_val), 1)

                component_dist = abs(feature_val - candidate_val) / range_val

            distance += weight * component_dist

        return distance

    def _aggregate_neighbors(self, neighbors: List[Tuple[Dict, float]],
                            cell_id: str, window_start: int, window_end: int,
                            context: Dict[str, Any]) -> Dict:
        """Aggregate K neighbors using weighted average based on distance"""
        if not neighbors:
            return None

        # Calculate weights (inverse of distance, avoiding division by zero)
        distances = [d for _, d in neighbors]
        weights = [1.0 / (d + 0.001) for d in distances]
        total_weight = sum(weights)
        normalized_weights = [w / total_weight for w in weights]

        # Initialize result
        result = {
            'cell_index': cell_id,
            'sample_count': 0,
            'start_time': window_start,
            'end_time': window_end,
            'is_empty_window': True,
            'knn_filled': True,
            'knn_neighbors_used': len(neighbors),
        }

        # Add metadata if provided
        if 'metadata' in context:
            result.update(context['metadata'])

        # Aggregate each field
        fields = context.get('fields', [])
        for field in fields:
            field_values = {
                'min': [],
                'max': [],
                'mean': [],
                'std': [],
            }

            # Collect values from neighbors
            for (neighbor, _), weight in zip(neighbors, normalized_weights):
                if field in neighbor and isinstance(neighbor[field], dict):
                    for stat in ['min', 'max', 'mean', 'std']:
                        val = neighbor[field].get(stat)
                        if val is not None:
                            field_values[stat].append((val, weight))

            # Compute weighted averages
            result[field] = {}
            for stat in ['min', 'max', 'mean', 'std']:
                if field_values[stat]:
                    weighted_avg = sum(val * w for val, w in field_values[stat])
                    result[field][stat] = weighted_avg
                else:
                    result[field][stat] = None

            result[field]['samples'] = 0  # Mark as imputed

        return result

    def _fallback_fill(self, cell_id: str, window_start: int, window_end: int,
                      context: Dict[str, Any]) -> Optional[Dict]:
        """Fallback to forward fill when no neighbors available"""
        if 'last_values' in context:
            # Use forward fill as fallback
            forward_fill = ForwardFillStrategy()
            result = forward_fill.handle(cell_id, window_start, window_end, context)
            if result:
                result['knn_fallback'] = 'forward_fill'
            return result

        # Last resort: zero fill
        zero_fill = ZeroFillStrategy()
        result = zero_fill.handle(cell_id, window_start, window_end, context)
        if result:
            result['knn_fallback'] = 'zero_fill'
        return result
