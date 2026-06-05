"""
Base transformer interface.
"""
from abc import ABC, abstractmethod
from typing import Any


class BaseTransformer(ABC):
    """
    Abstract base class for data transformers.
    """

    @abstractmethod
    def transform(self, data: dict[str, Any]) -> dict[str, Any]:
        """
        Transform the input data.

        Args:
            data: Input data dictionary

        Returns:
            Transformed data dictionary
        """
        pass

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}()"
