from abc import ABC, abstractmethod
from typing import Any


class EmptyWindowStrategy(ABC):
    @abstractmethod
    def handle(self, group_id: str, window_start: int, window_end: int,
               context: dict[str, Any] | None = None) -> dict | None:
        raise NotImplementedError


class SkipStrategy(EmptyWindowStrategy):
    """Skip empty windows — produce no output."""

    def handle(self, group_id: str, window_start: int, window_end: int,
               context: dict[str, Any] | None = None) -> dict | None:
        return None


class ZeroFillStrategy(EmptyWindowStrategy):
    """Emit a record for empty windows with zero/null stats for all known fields."""

    def handle(self, group_id: str, window_start: int, window_end: int,
               context: dict[str, Any] | None = None) -> dict | None:
        if not context:
            return None
        tags = context.get("tags", {})
        fields = context.get("fields", [])
        metrics = {
            field: {"mean": None, "min": None, "max": None, "std": None, "count": 0}
            for field in fields
        }
        return {
            "tags": tags,
            "window_start": window_start,
            "window_end": window_end,
            "window_duration_seconds": context.get("window_duration_seconds", 0),
            "sample_count": 0,
            "metrics": metrics,
        }
