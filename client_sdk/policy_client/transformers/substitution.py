"""
Substitution transformer - replaces field values with constants.
Note: Consider using redaction instead for type-preserving value replacement.
"""
from typing import Any
from policy_client.transformers.base import BaseTransformer


class SubstitutionTransformer(BaseTransformer):
    """
    Replace field values with specified constants, preserving data types.
    """

    def __init__(self, substitutions: dict[str, Any]):
        """
        Initialize the substitution transformer.

        Args:
            substitutions: Dictionary mapping field names to replacement values
                          (will be converted to match original field type)
        """
        self.substitutions = substitutions

    def transform(self, data: dict[str, Any]) -> dict[str, Any]:
        """
        Apply value substitutions to data, preserving original data types.

        Args:
            data: Input data dictionary

        Returns:
            Data with substituted values (same type as original)
        """
        result = data.copy()
        for field, replacement in self.substitutions.items():
            if field in result:
                original_value = result[field]

                # Convert replacement to match original type
                if isinstance(original_value, int):
                    result[field] = int(replacement)
                elif isinstance(original_value, float):
                    result[field] = float(replacement)
                elif isinstance(original_value, bool):
                    result[field] = bool(replacement)
                else:
                    result[field] = str(replacement)
        return result

    def __repr__(self) -> str:
        return f"SubstitutionTransformer(substitutions={len(self.substitutions)})"
