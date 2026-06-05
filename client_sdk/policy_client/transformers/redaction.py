"""
Redaction transformer - replaces sensitive values with placeholder.
"""
from typing import Any
from policy_client.transformers.base import BaseTransformer


class RedactionTransformer(BaseTransformer):
    """
    Replace sensitive field values with a placeholder, preserving data types.
    Uses -1 for int, -1.0 for float, False for bool, "***" for string/others.
    """

    def __init__(self, fields: list[str], replacement: str = "***"):
        """
        Initialize the redaction transformer.

        Args:
            fields: List of field names to redact
            replacement: String replacement for string fields (default: "***")
        """
        self.fields = set(fields)
        self.replacement = replacement

    def transform(self, data: dict[str, Any]) -> dict[str, Any]:
        """
        Apply redaction to specified fields, preserving data types.

        Args:
            data: Input data dictionary

        Returns:
            Data with redacted values (same type as original)
        """
        result = data.copy()
        for field in self.fields:
            if field in result:
                original_value = result[field]

                # Preserve the original data type
                if isinstance(original_value, int):
                    result[field] = -1
                elif isinstance(original_value, float):
                    result[field] = -1.0
                elif isinstance(original_value, bool):
                    result[field] = False
                else:
                    # For strings and other types, use replacement string
                    result[field] = self.replacement
        return result

    def __repr__(self) -> str:
        return f"RedactionTransformer(fields={len(self.fields)}, replacement='{self.replacement}')"
