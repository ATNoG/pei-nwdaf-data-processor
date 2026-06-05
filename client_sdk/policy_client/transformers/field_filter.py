"""
Field filtering transformer - whitelist/blacklist field filtering.
"""
from typing import Any, Literal
from policy_client.transformers.base import BaseTransformer


class FieldFilterTransformer(BaseTransformer):
    """
    Filter data fields based on whitelist or blacklist mode.
    """

    def __init__(
        self,
        mode: Literal["whitelist", "blacklist"],
        fields: list[str]
    ):
        """
        Initialize the field filter transformer.

        Args:
            mode: "whitelist" to only allow specified fields, "blacklist" to block them
            fields: List of field names to filter
        """
        self.mode = mode
        self.fields = set(fields)

    def transform(self, data: dict[str, Any]) -> dict[str, Any]:
        """
        Apply field filtering to data.

        Args:
            data: Input data dictionary

        Returns:
            Filtered data dictionary
        """
        if self.mode == "whitelist":
            return {k: v for k, v in data.items() if k in self.fields}
        else:  # blacklist
            return {k: v for k, v in data.items() if k not in self.fields}

    def __repr__(self) -> str:
        return f"FieldFilterTransformer(mode='{self.mode}', fields={len(self.fields)})"
