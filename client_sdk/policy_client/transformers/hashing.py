"""
Hashing transformer - SHA-256 hashing of sensitive values.
"""
import hashlib
from typing import Any
from policy_client.transformers.base import BaseTransformer


class HashingTransformer(BaseTransformer):
    """
    Hash sensitive field values using SHA-256 with optional salt.
    Preserves the original data type (int -> int, float -> float, str -> str).
    """

    def __init__(self, fields: list[str], salt: str = ""):
        """
        Initialize the hashing transformer.

        Args:
            fields: List of field names to hash
            salt: Salt string to prepend before hashing (default: "")
        """
        self.fields = set(fields)
        self.salt = salt.encode() if salt else b""

    def transform(self, data: dict[str, Any]) -> dict[str, Any]:
        """
        Apply SHA-256 hashing to specified fields, preserving data types.

        Args:
            data: Input data dictionary

        Returns:
            Data with hashed values (same type as original)
        """
        result = data.copy()
        for field in self.fields:
            if field in result:
                original_value = result[field]
                value = str(original_value).encode()
                hash_obj = hashlib.sha256(self.salt + value)
                hash_hex = hash_obj.hexdigest()

                # Preserve the original data type
                if isinstance(original_value, int):
                    # Convert first 8 chars of hex to int (32 bits)
                    result[field] = int(hash_hex[:8], 16)
                elif isinstance(original_value, float):
                    # Convert to int then float for consistency
                    result[field] = float(int(hash_hex[:8], 16))
                elif isinstance(original_value, bool):
                    # Use odd/even of hash for boolean
                    result[field] = bool(int(hash_hex[:8], 16) % 2)
                else:
                    # For strings and other types, use hex string
                    result[field] = hash_hex
        return result

    def __repr__(self) -> str:
        has_salt = bool(self.salt)
        return f"HashingTransformer(fields={len(self.fields)}, salted={has_salt})"
