from abc import ABC, abstractmethod
from typing import Any
from datetime import datetime
from collections import defaultdict


class EmptyWindowStrategy(ABC):
    """Base strategy for handling empty windows"""

    @abstractmethod
    def handle(self, cell_id: str, window_start: int, window_end: int,
               context: dict[str, Any] | None = None) -> dict | None:
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
               context: dict[str, Any] | None = None) -> dict | None:
        """Return None to skip processing this window"""
        return None


class ZeroFillStrategy(EmptyWindowStrategy):
    """Fill empty windows with zero/null values"""

    def handle(self, cell_id: str, window_start: int, window_end: int,
               context: dict[str, Any] | None = None) -> dict | None:
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
               context: dict[str, Any] | None = None) -> dict | None:
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
    """
    Fill empty windows using Time-based K-Nearest Neighbors.

    This simplified KNN approach uses only temporal features (hour, day_of_week, is_weekend)
    to find similar historical windows. It provides the best accuracy with minimal complexity.

    Replaces the original full KNN which used 10+ features with manual weights.
    See docs/STRATEGY_COMPARISON_REPORT.md for detailed comparison.
    """

    def __init__(self, k: int = 5, max_history_hours: int = 168,
                 history_buffer: dict[str, list[dict]] | None = None):
        """
        Initialize Time-based KNN strategy.

        Args:
            k: Number of nearest neighbors to use (default: 5)
            max_history_hours: Maximum hours of history to search (default: 1 week)
            history_buffer: External history buffer (dict[cell_id -> list of windows])
                           If None, creates internal buffer (not recommended for production)
        """
        self.k = k
        self.max_history_seconds = max_history_hours * 3600
        # Use external buffer if provided, otherwise create internal
        self.history_buffer = history_buffer if history_buffer is not None else defaultdict(list)
        self._owns_buffer = history_buffer is None

    def add_to_history(self, cell_id: str, window_data: dict[str, Any]):
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
               context: dict[str, Any] | None = None) -> dict | None:
        """
        Fill empty window using Time-based KNN.

        Context should contain:
            - fields: List of field names to fill
            - metadata: Dict of non-metric metadata (network, bandwidth, etc.)
            - last_values: Optional dict with last known processed values (for fallback)
        """
        if not context:
            return None

        # Extract temporal features
        features = self._extract_temporal_features(window_start)

        # Find K nearest neighbors from history
        candidates = self._get_candidates(cell_id, window_start)
        if len(candidates) < 1:
            # Fallback to forward fill if no history available
            return self._fallback_fill(cell_id, window_start, window_end, context)

        neighbors = self._find_knn(features, candidates)
        if not neighbors:
            return self._fallback_fill(cell_id, window_start, window_end, context)

        # Generate filled window by averaging neighbors
        filled_window = self._aggregate_neighbors(
            neighbors, cell_id, window_start, window_end, context
        )

        return filled_window

    def _extract_temporal_features(self, timestamp: int) -> dict[str, Any]:
        """Extract only temporal features for similarity matching."""
        dt = datetime.fromtimestamp(timestamp)
        return {
            'hour': dt.hour,
            'day_of_week': dt.weekday(),
            'is_weekend': dt.weekday() >= 5,
        }

    def _get_candidates(self, cell_id: str, current_time: int) -> list[dict]:
        """Get candidate windows from history (only from the past)."""
        candidates = []

        # Convert to string for consistent keying
        cell_key = str(cell_id)

        # Get windows from the same cell, only from before current_time
        for window in self.history_buffer.get(cell_key, []):
            if window.get('window_end', 0) < current_time:
                candidates.append(window)

        return candidates

    def _find_knn(self, features: dict[str, Any], candidates: list[dict]) -> list[tuple[dict, float]]:
        """Find K nearest neighbors based on temporal distance."""
        distances = []

        for candidate in candidates:
            distance = self._calculate_temporal_distance(features, candidate)
            distances.append((candidate, distance))

        # Sort by distance and take top K
        distances.sort(key=lambda x: x[1])
        return distances[:self.k]

    def _calculate_temporal_distance(self, features: dict[str, Any], candidate: dict[str, Any]) -> float:
        """
        Calculate distance based on temporal features only.

        Uses:
        - Circular distance for hour (handles 23:00 vs 00:00 correctly)
        - Binary distance for weekend vs weekday
        """
        distance = 0.0

        # Get candidate's temporal info from window_end timestamp
        candidate_time = candidate.get('window_end', 0)
        candidate_dt = datetime.fromtimestamp(candidate_time)

        # Hour distance (circular: 23 and 0 are close)
        hour_diff = abs(features['hour'] - candidate_dt.hour)
        hour_dist = min(hour_diff, 24 - hour_diff) / 12.0  # Normalize to [0, 1]
        distance += hour_dist

        # Day of week distance (weekend vs weekday is more important than specific day)
        is_weekend_candidate = candidate_dt.weekday() >= 5
        day_dist = 0.0 if features['is_weekend'] == is_weekend_candidate else 0.5
        distance += day_dist

        return distance

    def _aggregate_neighbors(self, neighbors: list[tuple[dict, float]],
                            cell_id: str, window_start: int, window_end: int,
                            context: dict[str, Any]) -> dict:
        """Aggregate K neighbors using simple average (all neighbors equal weight)."""
        if not neighbors:
            return None

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

        # Aggregate each field using simple average
        fields = context.get('fields', [])
        for field in fields:
            field_values = {
                'min': [],
                'max': [],
                'mean': [],
                'std': [],
            }

            # Collect values from all neighbors (equal weight)
            for neighbor, _ in neighbors:
                if field in neighbor and isinstance(neighbor[field], dict):
                    for stat in ['min', 'max', 'mean', 'std']:
                        val = neighbor[field].get(stat)
                        if val is not None:
                            field_values[stat].append(val)

            # Compute simple averages
            result[field] = {}
            for stat in ['min', 'max', 'mean', 'std']:
                if field_values[stat]:
                    result[field][stat] = sum(field_values[stat]) / len(field_values[stat])
                else:
                    result[field][stat] = None

            result[field]['samples'] = 0  # Mark as imputed

        return result

    def _fallback_fill(self, cell_id: str, window_start: int, window_end: int,
                      context: dict[str, Any]) -> dict | None:
        """Fallback to forward fill when no neighbors available."""
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
