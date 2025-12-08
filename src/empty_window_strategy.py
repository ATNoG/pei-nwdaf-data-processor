from enum import Enum

class EmptyWindowStrategy(Enum):
    """Strategy for handling empty windows"""
    SKIP = "skip"              # Don't process empty windows

    # Other possibilities to implement later
    ZERO_FILL = "zero_fill"    # Fill the record with zero values
    FORWARD_FILL = "forward_fill"  # Forward the last known values
