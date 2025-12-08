from abc import ABC, abstractmethod
from src.empty_window_strategy import EmptyWindowStrategy

class ProcessingProfile(ABC):
    @classmethod
    @abstractmethod
    def process(cls, data:list[dict]) -> dict|None:
       raise NotImplementedError

    @classmethod
    @abstractmethod
    def handle_empty_window(cls, cell_id: str, window_start: int, window_end: int, strategy: EmptyWindowStrategy) -> dict|None:
        raise NotImplementedError
