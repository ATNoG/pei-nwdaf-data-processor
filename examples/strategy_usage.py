"""
Example demonstrating the Strategy pattern for empty window handling.
This file was 100% VIBECODED.
"""

from src.time_window_manager import TimeWindowManager
from src.profiles.latency_profile import LatencyProfile
from src.empty_window_strategy import SkipStrategy, ZeroFillStrategy, ForwardFillStrategy


def window_callback(cell_id, processed_data, start, end):
    """Callback when a window is completed"""
    print(f"Window [{start}-{end}] for cell {cell_id}:")
    print(f"  Data: {processed_data}")
    print()


# Example 1: Using SKIP strategy (default - empty windows are ignored)
print("=" * 60)
print("Example 1: SKIP Strategy")
print("=" * 60)
manager_skip = TimeWindowManager(
    window_duration_seconds=10,
    on_window_complete=window_callback,
    processing_profile=LatencyProfile,
    empty_window_strategy=SkipStrategy()
)

# Example 2: Using ZERO_FILL strategy (empty windows get zero values)
print("=" * 60)
print("Example 2: ZERO_FILL Strategy")
print("=" * 60)
manager_zero = TimeWindowManager(
    window_duration_seconds=10,
    on_window_complete=window_callback,
    processing_profile=LatencyProfile,
    empty_window_strategy=ZeroFillStrategy()
)

# Example 3: Using FORWARD_FILL strategy (empty windows use last known values)
print("=" * 60)
print("Example 3: FORWARD_FILL Strategy")
print("=" * 60)
manager_forward = TimeWindowManager(
    window_duration_seconds=10,
    on_window_complete=window_callback,
    processing_profile=LatencyProfile,
    empty_window_strategy=ForwardFillStrategy()
)

# You can also swap strategies at runtime:
print("=" * 60)
print("Example 4: Runtime Strategy Switching")
print("=" * 60)
manager = TimeWindowManager(
    window_duration_seconds=10,
    processing_profile=LatencyProfile,
    empty_window_strategy=SkipStrategy()
)

# Process some data with SKIP
print("Using SKIP strategy...")
# ... process data ...

# Switch to ZERO_FILL
manager.empty_window_strategy = ZeroFillStrategy()
print("Switched to ZERO_FILL strategy...")
# ... process more data ...

# Switch to FORWARD_FILL
manager.empty_window_strategy = ForwardFillStrategy()
print("Switched to FORWARD_FILL strategy...")
# ... process more data ...

print("\n✅ Strategy pattern allows flexible, runtime-swappable behavior!")
