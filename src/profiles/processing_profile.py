from abc import ABC, abstractmethod
from typing import Any
from src.empty_window_strategy import EmptyWindowStrategy


class ProcessingProfile(ABC):
    @classmethod
    @abstractmethod
    def process(cls, data: list[dict]) -> dict | None:
        """Process a window of measurements and return aggregated data"""
        raise NotImplementedError

    @classmethod
    @abstractmethod
    def get_empty_window_context(cls, cell_id: str, last_processed: dict | None = None) -> dict[str, Any]:
        """
        Provide profile-specific context for empty window handling.

        Args:
            cell_id: The cell identifier
            last_processed: The last successfully processed window data for this cell

        Returns:
            Dictionary with context data needed by the strategy (fields, metadata, last_values, etc.)
        """
        raise NotImplementedError

    @classmethod
    def handle_empty_window(cls, cell_id: str, window_start: int, window_end: int,
                           strategy: EmptyWindowStrategy, last_processed: dict | None = None) -> dict | None:
        """
        Handle an empty window using the provided strategy.

        Args:
            cell_id: Cell identifier
            window_start: Window start timestamp
            window_end: Window end timestamp
            strategy: Strategy instance to use for handling
            last_processed: Last successfully processed window data for this cell

        Returns:
            Processed empty window data or None
        """
        context = cls.get_empty_window_context(cell_id, last_processed)
        return strategy.handle(cell_id, window_start, window_end, context)
