"""
Comprehensive tests for KNN-based empty window filling strategies.

Tests cover:
- Time-based KNN (current implementation)
- History management
- Fallback behavior
- Edge cases
- Integration with TimeWindowManager
"""
import pytest
from unittest.mock import Mock, patch, AsyncMock
from datetime import datetime
from collections import defaultdict
from src.empty_window_strategy import (
    KNNStrategy,
    SkipStrategy,
    ZeroFillStrategy,
    ForwardFillStrategy,
)
from src.time_window_manager import TimeWindowManager
from src.profiles.latency_profile import LatencyProfile


class MockStorage:
    """Mock storage structure for testing"""
    url = "http://mock-storage/api/v1/"

    class endpoint:
        cell = "cell/"
        raw = "raw/"


# ============================================================================
# TIME-BASED KNN STRATEGY TESTS
# ============================================================================

class TestTimeBasedKNNStrategy:
    """Test suite for TimeBasedKNNStrategy (KNNStrategy class)."""

    def test_init_default_parameters(self):
        """Test KNNStrategy initialization with default parameters."""
        strategy = KNNStrategy()
        assert strategy.k == 5
        assert strategy.max_history_seconds == 168 * 3600  # 1 week
        assert strategy._owns_buffer is True

    def test_init_custom_parameters(self):
        """Test KNNStrategy initialization with custom parameters."""
        buffer = defaultdict(list)
        strategy = KNNStrategy(k=10, max_history_hours=72, history_buffer=buffer)
        assert strategy.k == 10
        assert strategy.max_history_seconds == 72 * 3600  # 3 days
        assert strategy.history_buffer is buffer
        assert strategy._owns_buffer is False

    def test_extract_temporal_features(self):
        """Test temporal feature extraction."""
        strategy = KNNStrategy()

        # Test Tuesday 14:30 (weekday, afternoon)
        dt = datetime(2024, 1, 9, 14, 30, 0)  # Jan 9, 2024 is a Tuesday
        timestamp = int(dt.timestamp())
        features = strategy._extract_temporal_features(timestamp)

        assert features['hour'] == 14
        assert features['day_of_week'] == 1  # Tuesday
        assert features['is_weekend'] is False

        # Test Saturday 23:45 (weekend, late night)
        dt = datetime(2024, 1, 13, 23, 45, 0)  # Jan 13, 2024 is a Saturday
        timestamp = int(dt.timestamp())
        features = strategy._extract_temporal_features(timestamp)

        assert features['hour'] == 23
        assert features['day_of_week'] == 5  # Saturday
        assert features['is_weekend'] is True

    def test_calculate_temporal_distance_same_time(self):
        """Test temporal distance calculation for identical times."""
        strategy = KNNStrategy()

        # Tuesday 14:00
        dt = datetime(2024, 1, 9, 14, 0, 0)
        timestamp = int(dt.timestamp())
        features = strategy._extract_temporal_features(timestamp)

        # Candidate from same time
        candidate = {
            'window_end': timestamp,
            'rsrp': {'mean': -80},
            'sinr': {'mean': 20},
        }

        distance = strategy._calculate_temporal_distance(features, candidate)
        assert distance == 0.0  # Same time = no distance

    def test_calculate_temporal_distance_circular_hour(self):
        """Test that hour distance is circular (23:00 close to 00:00)."""
        strategy = KNNStrategy()

        # 23:00 (11 PM)
        dt = datetime(2024, 1, 9, 23, 0, 0)
        timestamp = int(dt.timestamp())
        features = strategy._extract_temporal_features(timestamp)

        # Candidate from 00:00 (12 AM) next day - should be close
        candidate_timestamp = timestamp + 3600  # 1 hour later
        candidate = {
            'window_end': candidate_timestamp,
            'rsrp': {'mean': -80},
        }

        distance = strategy._calculate_temporal_distance(features, candidate)
        assert distance < 0.2  # Should be small (circular distance)

        # Candidate from 12:00 (noon) - should be far
        candidate_timestamp = timestamp + 39600  # 11 hours later (12:00 next day)
        candidate = {
            'window_end': candidate_timestamp,
            'rsrp': {'mean': -80},
        }

        distance = strategy._calculate_temporal_distance(features, candidate)
        assert distance > 0.8  # Should be large

    def test_calculate_temporal_distance_weekend_vs_weekday(self):
        """Test weekend vs weekday distance calculation."""
        strategy = KNNStrategy()

        # Tuesday (weekday)
        dt = datetime(2024, 1, 9, 14, 0, 0)
        timestamp = int(dt.timestamp())
        features = strategy._extract_temporal_features(timestamp)

        # Candidate from Saturday (weekend)
        candidate_dt = datetime(2024, 1, 13, 14, 0, 0)
        candidate_timestamp = int(candidate_dt.timestamp())
        candidate = {
            'window_end': candidate_timestamp,
            'rsrp': {'mean': -80},
        }

        distance = strategy._calculate_temporal_distance(features, candidate)
        assert distance >= 0.5  # Weekend vs weekday should have penalty

        # Candidate from another Tuesday (weekday)
        candidate_dt = datetime(2024, 1, 16, 14, 0, 0)
        candidate_timestamp = int(candidate_dt.timestamp())
        candidate = {
            'window_end': candidate_timestamp,
            'rsrp': {'mean': -80},
        }

        distance = strategy._calculate_temporal_distance(features, candidate)
        assert distance < 0.5  # Same type of day should be closer

    def test_add_to_history(self):
        """Test adding windows to history."""
        strategy = KNNStrategy(k=3)
        cell_id = "1"

        # Add a normal window
        window = {
            'cell_index': 1,
            'window_end': 1000,
            'is_empty_window': False,
            'rsrp': {'mean': -80},
            'sinr': {'mean': 20},
        }
        strategy.add_to_history(cell_id, window)

        assert len(strategy.history_buffer[cell_id]) == 1

        # Try to add an empty window - should be ignored
        empty_window = {
            'cell_index': 1,
            'window_end': 1100,
            'is_empty_window': True,
        }
        strategy.add_to_history(cell_id, empty_window)

        assert len(strategy.history_buffer[cell_id]) == 1  # Still 1

    def test_history_pruning(self):
        """Test that old history is pruned automatically."""
        strategy = KNNStrategy(k=3, max_history_hours=1)  # 1 hour history
        cell_id = "1"

        # Add old window (2 hours ago)
        old_time = 1000
        old_window = {
            'cell_index': 1,
            'window_end': old_time,
            'is_empty_window': False,
            'rsrp': {'mean': -80},
        }
        strategy.add_to_history(cell_id, old_window)

        # Add recent window
        recent_time = old_time + 7200  # 2 hours later
        recent_window = {
            'cell_index': 1,
            'window_end': recent_time,
            'is_empty_window': False,
            'rsrp': {'mean': -75},
        }
        strategy.add_to_history(cell_id, recent_window)

        # Old window should be pruned
        assert len(strategy.history_buffer[cell_id]) == 1
        assert strategy.history_buffer[cell_id][0]['window_end'] == recent_time

    def test_get_candidates(self):
        """Test getting candidate windows from history."""
        strategy = KNNStrategy(k=3)
        cell_id = "1"

        # Add some historical windows
        for i in range(5):
            window = {
                'cell_index': 1,
                'window_end': 1000 + i * 100,
                'is_empty_window': False,
                'rsrp': {'mean': -80 + i},
                'sinr': {'mean': 20 - i},
            }
            strategy.add_to_history(cell_id, window)

        # Get candidates for current time
        current_time = 1600
        candidates = strategy._get_candidates(cell_id, current_time)

        assert len(candidates) == 5  # All windows are from the past

    def test_find_knn(self):
        """Test finding K nearest neighbors."""
        strategy = KNNStrategy(k=3)
        cell_id = "1"

        # Target: Tuesday 14:00
        dt = datetime(2024, 1, 9, 14, 0, 0)
        target_timestamp = int(dt.timestamp())
        features = strategy._extract_temporal_features(target_timestamp)

        # Add candidates at different times
        candidates = []

        # Same time last week - should be closest
        candidates.append({
            'window_end': target_timestamp - 7 * 86400,
            'rsrp': {'mean': -80},
            'sinr': {'mean': 20},
        })

        # Different time - should be farther
        dt_diff = datetime(2024, 1, 9, 2, 0, 0)  # 2 AM
        candidates.append({
            'window_end': int(dt_diff.timestamp()),
            'rsrp': {'mean': -75},
            'sinr': {'mean': 15},
        })

        # Weekend - should be farthest
        dt_weekend = datetime(2024, 1, 13, 14, 0, 0)  # Saturday
        candidates.append({
            'window_end': int(dt_weekend.timestamp()),
            'rsrp': {'mean': -85},
            'sinr': {'mean': 18},
        })

        neighbors = strategy._find_knn(features, candidates)

        assert len(neighbors) == 3  # k=3, so return all
        # First should be closest (same time of day)
        assert neighbors[0][1] < neighbors[1][1]

    def test_aggregate_neighbors(self):
        """Test aggregating neighbor values."""
        strategy = KNNStrategy(k=2)
        cell_id = "1"

        neighbors = [
            (
                {
                    'rsrp': {'mean': -80, 'min': -85, 'max': -75, 'std': 3.0},
                    'sinr': {'mean': 20, 'min': 15, 'max': 25, 'std': 4.0},
                },
                0.1,  # distance
            ),
            (
                {
                    'rsrp': {'mean': -82, 'min': -87, 'max': -77, 'std': 3.5},
                    'sinr': {'mean': 18, 'min': 13, 'max': 23, 'std': 4.5},
                },
                0.2,  # distance
            ),
        ]

        context = {
            'fields': ['rsrp', 'sinr'],
            'metadata': {'network': 'test-net', 'primary_bandwidth': 20000},
        }

        result = strategy._aggregate_neighbors(
            neighbors, cell_id, 1000, 1100, context
        )

        assert result['cell_index'] == cell_id
        assert result['is_empty_window'] is True
        assert result['knn_filled'] is True
        assert result['knn_neighbors_used'] == 2
        assert result['network'] == 'test-net'

        # Check simple average (not weighted)
        expected_rsrp = (-80 + -82) / 2
        expected_sinr = (20 + 18) / 2
        assert abs(result['rsrp']['mean'] - expected_rsrp) < 0.01
        assert abs(result['sinr']['mean'] - expected_sinr) < 0.01

    def test_handle_empty_window_with_history(self):
        """Test filling an empty window when history exists."""
        strategy = KNNStrategy(k=2)
        cell_id = "1"

        # Build history - Tuesday 14:00
        dt = datetime(2024, 1, 9, 14, 0, 0)
        history_timestamp = int(dt.timestamp())

        for i in range(5):
            window = {
                'cell_index': 1,
                'window_end': history_timestamp + i * 100,
                'is_empty_window': False,
                'rsrp': {'mean': -80 + i, 'min': -85 + i, 'max': -75 + i, 'std': 2.0},
                'sinr': {'mean': 20 - i, 'min': 15 - i, 'max': 25 - i, 'std': 3.0},
                'cqi': {'mean': 10, 'min': 8, 'max': 12, 'std': 1.5},
            }
            strategy.add_to_history(cell_id, window)

        # Fill empty window at similar time
        dt_target = datetime(2024, 1, 9, 14, 30, 0)
        window_start = int(dt_target.timestamp())
        window_end = window_start + 60

        context = {
            'fields': ['rsrp', 'sinr', 'cqi'],
            'metadata': {'network': 'test-net', 'primary_bandwidth': 20000},
        }

        result = strategy.handle(cell_id, window_start, window_end, context)

        assert result is not None
        assert result['knn_filled'] is True
        assert result['sample_count'] == 0
        assert result['is_empty_window'] is True
        assert 'rsrp' in result
        assert result['rsrp']['mean'] is not None

    def test_handle_empty_window_no_history(self):
        """Test fallback to forward fill when no history exists."""
        strategy = KNNStrategy(k=2)
        cell_id = "1"

        window_start = 1000
        window_end = 1100

        last_values = {
            'cell_index': 1,
            'rsrp': {'mean': -80, 'min': -85, 'max': -75, 'std': 2.0},
            'sinr': {'mean': 20, 'min': 15, 'max': 25, 'std': 3.0},
            'network': 'test-net',
        }

        context = {
            'fields': ['rsrp', 'sinr'],
            'metadata': {'network': 'test-net'},
            'last_values': last_values,
        }

        result = strategy.handle(cell_id, window_start, window_end, context)

        assert result is not None
        assert result.get('knn_fallback') == 'forward_fill'
        assert result['forward_filled'] is True
        # Should have last values
        assert result['rsrp']['mean'] == -80

    def test_handle_empty_window_no_history_no_last_values(self):
        """Test fallback to zero fill when no history and no last values."""
        strategy = KNNStrategy(k=2)
        cell_id = "1"

        window_start = 1000
        window_end = 1100

        context = {
            'fields': ['rsrp', 'sinr'],
            'metadata': {'network': 'test-net'},
        }

        result = strategy.handle(cell_id, window_start, window_end, context)

        assert result is not None
        assert result.get('knn_fallback') == 'zero_fill'
        # Should have None values
        assert result['rsrp']['mean'] is None

    def test_handle_empty_window_no_context(self):
        """Test handling with no context returns None."""
        strategy = KNNStrategy(k=2)
        result = strategy.handle("1", 1000, 1100, None)
        assert result is None


# ============================================================================
# INTEGRATION TESTS WITH TIMEWINDOWMANAGER
# ============================================================================

class TestKNNIntegration:
    """Integration tests for KNN strategy with TimeWindowManager."""

    @pytest.mark.asyncio
    @patch('httpx.AsyncClient.get', new_callable=AsyncMock)
    async def test_knn_with_time_window_manager(self, mock_get):
        """Test KNN strategy integration with TimeWindowManager."""
        results = []

        # Mock cells endpoint
        mock_cells_response = Mock()
        mock_cells_response.json.return_value = [1]
        mock_cells_response.raise_for_status = Mock()

        # First window: has data
        mock_data_window1 = Mock()
        mock_data_window1.json.return_value = {
            "data": [
                {
                    "cell_index": 1,
                    "timestamp": 5,
                    "rsrp": -80,
                    "sinr": 20.0,
                    "rsrq": -10,
                    "mean_latency": 15,
                    "cqi": 10,
                    "network": "net1",
                    "primary_bandwidth": 20000,
                    "ul_bandwidth": 20000,
                }
            ],
            "has_next": False
        }
        mock_data_window1.raise_for_status = Mock()

        # Second window: empty - should use KNN
        mock_empty_data = Mock()
        mock_empty_data.json.return_value = {"data": [], "has_next": False}
        mock_empty_data.raise_for_status = Mock()

        mock_get.side_effect = [
            mock_cells_response,  # get cells
            mock_data_window1,  # window [0, 60) - has data
            mock_cells_response,  # get cells again
            mock_empty_data,  # window [60, 120) - empty, KNN should fill
        ]

        # Create history buffer for KNN
        from collections import defaultdict
        history_buffer = defaultdict(list)

        manager = TimeWindowManager(
            window_size=60,
            storage_struct=MockStorage,
            on_window_complete=lambda data: results.append(data),
            processing_profiles=[LatencyProfile()],
            empty_window_strategy=KNNStrategy(k=3, max_history_hours=168, history_buffer=history_buffer),
        )

        manager.set_initial_watermark(0)
        await manager.advance_watermark(60)  # First window with data
        await manager.advance_watermark(120)  # Second window empty

        # Should have 2 results (first with data, second filled by KNN)
        assert len(results) == 2
        assert results[0]['sample_count'] > 0  # First has data
        assert results[1]['is_empty_window'] is True  # Second is empty/filled

        # KNN should have filled it or fallen back
        if results[1].get('knn_filled'):
            assert results[1]['knn_filled'] is True
        elif results[1].get('knn_fallback'):
            # With only 1 historical window, KNN might fall back
            assert results[1]['knn_fallback'] in ['forward_fill', 'zero_fill']


# ============================================================================
# EDGE CASE TESTS
# ============================================================================

class TestKNNEdgeCases:
    """Test edge cases for KNN strategy."""

    def test_k_value_larger_than_history(self):
        """Test when k is larger than available history."""
        strategy = KNNStrategy(k=10)  # Ask for 10 neighbors
        cell_id = "1"

        # Only add 3 windows to history
        for i in range(3):
            window = {
                'cell_index': 1,
                'window_end': 1000 + i * 100,
                'is_empty_window': False,
                'rsrp': {'mean': -80 + i},
            }
            strategy.add_to_history(cell_id, window)

        # Get candidates
        candidates = strategy._get_candidates(cell_id, 2000)

        # Extract features
        features = strategy._extract_temporal_features(2000)

        # Find neighbors
        neighbors = strategy._find_knn(features, candidates)

        # Should return all available (3), not error
        assert len(neighbors) == 3

    def test_empty_history_buffer(self):
        """Test behavior with completely empty history."""
        strategy = KNNStrategy(k=3)

        context = {
            'fields': ['rsrp', 'sinr'],
            'metadata': {'network': 'test-net'},
            'last_values': {
                'cell_index': 1,
                'rsrp': {'mean': -80},
            },
        }

        result = strategy.handle("1", 1000, 1100, context)

        # Should fall back to forward fill
        assert result is not None
        assert result.get('knn_fallback') == 'forward_fill'

    def test_multiple_cells_separate_history(self):
        """Test that different cells maintain separate history."""
        strategy = KNNStrategy(k=2)

        # Add windows for cell 1
        for i in range(3):
            window = {
                'cell_index': 1,
                'window_end': 1000 + i * 100,
                'is_empty_window': False,
                'rsrp': {'mean': -80 + i},
            }
            strategy.add_to_history("1", window)

        # Add windows for cell 2
        for i in range(2):
            window = {
                'cell_index': 2,
                'window_end': 2000 + i * 100,
                'is_empty_window': False,
                'rsrp': {'mean': -90 + i},
            }
            strategy.add_to_history("2", window)

        # Check cell 1 history
        candidates_1 = strategy._get_candidates("1", 2000)
        assert len(candidates_1) == 3

        # Check cell 2 history
        candidates_2 = strategy._get_candidates("2", 3000)
        assert len(candidates_2) == 2

    def test_history_with_none_values(self):
        """Test handling candidates with None metric values."""
        strategy = KNNStrategy(k=2)
        cell_id = "1"

        # Add window with some None values
        window = {
            'cell_index': 1,
            'window_end': 1000,
            'is_empty_window': False,
            'rsrp': {'mean': -80, 'min': None, 'max': None, 'std': None},
            'sinr': {'mean': None, 'min': None, 'max': None, 'std': None},
        }
        strategy.add_to_history(cell_id, window)

        context = {
            'fields': ['rsrp', 'sinr'],
            'metadata': {'network': 'test-net'},
        }

        result = strategy.handle(cell_id, 1100, 1200, context)

        # Should handle None values gracefully
        if result and result.get('knn_filled'):
            # RSRP should have a value, SINR might be None
            assert result['rsrp']['mean'] == -80
            assert result['sinr']['mean'] is None

    def test_window_at_midnight(self):
        """Test temporal distance calculation at midnight (edge case for circular distance)."""
        strategy = KNNStrategy()

        # 00:00 (midnight)
        dt = datetime(2024, 1, 9, 0, 0, 0)
        timestamp = int(dt.timestamp())
        features = strategy._extract_temporal_features(timestamp)

        # Candidate from 23:00 previous day
        candidate_timestamp = timestamp - 3600
        candidate = {
            'window_end': candidate_timestamp,
            'rsrp': {'mean': -80},
        }

        distance = strategy._calculate_temporal_distance(features, candidate)
        # Should be small (only 1 hour apart on circular clock)
        assert distance < 0.2

    def test_very_large_k_value(self):
        """Test with very large k value."""
        strategy = KNNStrategy(k=1000)  # Unrealistic but should not crash

        for i in range(10):
            window = {
                'cell_index': 1,
                'window_end': 1000 + i * 100,
                'is_empty_window': False,
                'rsrp': {'mean': -80 + i},
            }
            strategy.add_to_history("1", window)

        candidates = strategy._get_candidates("1", 3000)
        features = strategy._extract_temporal_features(3000)

        neighbors = strategy._find_knn(features, candidates)

        # Should return all 10 candidates (limit by available data)
        assert len(neighbors) == 10


# ============================================================================
# PERFORMANCE TESTS
# ============================================================================

class TestKNNPerformance:
    """Performance tests for KNN strategy."""

    def test_large_history_performance(self):
        """Test performance with large history buffer."""
        import time

        strategy = KNNStrategy(k=5)
        cell_id = "1"

        # Add 1000 historical windows
        for i in range(1000):
            window = {
                'cell_index': 1,
                'window_end': 1000 + i * 60,
                'is_empty_window': False,
                'rsrp': {'mean': -80 + (i % 20)},
                'sinr': {'mean': 20 - (i % 10)},
            }
            strategy.add_to_history(cell_id, window)

        # Measure time to find neighbors
        features = strategy._extract_temporal_features(61000)
        candidates = strategy._get_candidates(cell_id, 61000)

        start = time.time()
        neighbors = strategy._find_knn(features, candidates)
        elapsed = time.time() - start

        assert len(neighbors) == 5
        # Should complete in reasonable time (< 1 second even with 1000 candidates)
        assert elapsed < 1.0

    def test_memory_efficiency_with_pruning(self):
        """Test that history pruning prevents unbounded memory growth."""
        strategy = KNNStrategy(k=5, max_history_hours=1)
        cell_id = "1"

        # Add many windows spanning more than max_history_hours
        base_time = 1000
        for i in range(100):
            window = {
                'cell_index': 1,
                'window_end': base_time + i * 60,
                'is_empty_window': False,
                'rsrp': {'mean': -80},
            }
            strategy.add_to_history(cell_id, window)

        # History should be pruned to ~60 windows (1 hour worth)
        # Plus some tolerance for the pruning check
        assert len(strategy.history_buffer[cell_id]) < 70


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
