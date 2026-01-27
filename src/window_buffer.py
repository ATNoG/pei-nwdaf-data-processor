from collections import deque


class CellWindowBuffer:
    """
    A time-ordered buffer for raw measurement records of a single cell.
    Records are assumed to arrive in chronological order per append_slice call.
    """

    def __init__(self, time_field: str = "timestamp"):
        self._data: deque[dict] = deque()
        self._time_field = time_field

    def append_slice(self, records: list[dict]) -> None:
        self._data.extend(records)

    def evict_before(self, cutoff: int) -> None:
        """Remove all records whose time_field value is < cutoff."""
        while self._data and self._data[0].get(self._time_field, 0) < cutoff:
            self._data.popleft()

    def get_all(self) -> list[dict]:
        return list(self._data)

    def __len__(self) -> int:
        return len(self._data)

    def clear(self) -> None:
        self._data.clear()
