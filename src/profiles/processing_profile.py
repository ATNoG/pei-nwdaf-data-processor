from abc import ABC, abstractmethod


class ProcessingProfile(ABC):
    @classmethod
    @abstractmethod
    def process(cls, data:list[dict]) -> dict|None:
       raise NotImplementedError
