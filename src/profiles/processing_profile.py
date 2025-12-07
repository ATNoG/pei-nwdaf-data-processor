from abc import ABC, abstractmethod
from enum import Enum

class EmptyWindowStrategy(Enum):
    """Strategy for handling empty windows"""
    SKIP = "skip"              # Don't process empty windows

    # Other possibilities to implement later
    ZERO_FILL = "zero_fill"    # Fill the record with zero values
    FORWARD_FILL = "forward_fill"  # Forward the last known values


class ProcessingProfile(ABC):
    @classmethod
    @abstractmethod
    def process(cls, data:list[dict]) -> dict|None:
       raise NotImplementedError
    
    @classmethod
    @abstractmethod
    def handle_empty_window(cls, cell_id: str, window_start: int, window_end: int, strategy: EmptyWindowStrategy) -> dict|None:
        raise NotImplementedError
