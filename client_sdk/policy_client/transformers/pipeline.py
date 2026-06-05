"""
Transformer pipeline - orchestrates multiple transformations in sequence.
"""
from typing import Any
from policy_client.transformers.base import BaseTransformer
from policy_client.transformers.field_filter import FieldFilterTransformer
from policy_client.transformers.redaction import RedactionTransformer
from policy_client.transformers.hashing import HashingTransformer
from policy_client.transformers.substitution import SubstitutionTransformer
from policy_client.transformers.homomorphic import HomomorphicEncryptionTransformer, HomomorphicDecryptionTransformer


class TransformerPipeline:
    """
    Executes multiple transformers in sequence (chain of responsibility).
    """

    def __init__(self):
        self.transformers: list[BaseTransformer] = []

    def add_filter(self, mode: str, fields: list[str]) -> "TransformerPipeline":
        """
        Add a field filter transformer.

        Args:
            mode: "whitelist" or "blacklist"
            fields: List of field names

        Returns:
            Self for method chaining
        """
        self.transformers.append(
            FieldFilterTransformer(mode=mode, fields=fields)
        )
        return self

    def add_redaction(
        self,
        fields: list[str],
        replacement: str = "***"
    ) -> "TransformerPipeline":
        """
        Add a redaction transformer.

        Args:
            fields: List of field names to redact
            replacement: Replacement string

        Returns:
            Self for method chaining
        """
        self.transformers.append(
            RedactionTransformer(fields=fields, replacement=replacement)
        )
        return self

    def add_hashing(self, fields: list[str], salt: str = "") -> "TransformerPipeline":
        """
        Add a hashing transformer.

        Args:
            fields: List of field names to hash
            salt: Optional salt string

        Returns:
            Self for method chaining
        """
        self.transformers.append(
            HashingTransformer(fields=fields, salt=salt)
        )
        return self

    def add_substitution(
        self,
        substitutions: dict[str, Any]
    ) -> "TransformerPipeline":
        """
        Add a substitution transformer.

        Args:
            substitutions: Dict mapping field names to replacement values

        Returns:
            Self for method chaining
        """
        self.transformers.append(
            SubstitutionTransformer(substitutions=substitutions)
        )
        return self

    def add_transformer(self, transformer: BaseTransformer) -> "TransformerPipeline":
        """
        Add a custom transformer.

        Args:
            transformer: BaseTransformer instance

        Returns:
            Self for method chaining
        """
        self.transformers.append(transformer)
        return self

    def execute(self, data: dict[str, Any]) -> dict[str, Any]:
        """
        Execute all transformers in sequence.

        Args:
            data: Input data dictionary

        Returns:
            Transformed data dictionary
        """
        result = data
        for transformer in self.transformers:
            result = transformer.transform(result)
        return result

    def execute_sync(self, data: dict[str, Any]) -> dict[str, Any]:
        """
        Synchronous version of execute (now just calls execute directly).

        Args:
            data: Input data dictionary

        Returns:
            Transformed data dictionary
        """
        return self.execute(data)

    def clear(self) -> None:
        """Clear all transformers from the pipeline."""
        self.transformers.clear()

    @classmethod
    def from_config(cls, config: dict) -> "TransformerPipeline":
        """
        Build a pipeline from configuration dictionary.

        Args:
            config: Configuration dict with "steps" list

        Returns:
            Configured TransformerPipeline

        Example config:
        {
            "steps": [
                {"type": "filter", "params": {"mode": "whitelist", "fields": ["a", "b"]}},
                {"type": "redaction", "params": {"fields": ["c"], "replacement": "***"}}
            ]
        }
        """
        pipeline = cls()

        for step in config.get("steps", []):
            step_type = step["type"]
            params = step.get("params", {})

            if step_type == "filter":
                pipeline.add_filter(**params)
            elif step_type == "redaction":
                pipeline.add_redaction(**params)
            elif step_type == "hashing":
                pipeline.add_hashing(**params)
            elif step_type == "substitution":
                pipeline.add_substitution(**params)
            elif step_type == "homomorphic_encrypt":
                pipeline.add_transformer(HomomorphicEncryptionTransformer(**params))
            elif step_type == "homomorphic_decrypt":
                pipeline.add_transformer(HomomorphicDecryptionTransformer(**params))
            else:
                raise ValueError(f"Unknown transformer type: {step_type}")

        return pipeline

    def to_config(self) -> dict:
        """
        Export pipeline configuration to dictionary.

        Returns:
            Configuration dict that can be used with from_config
        """
        steps = []
        for transformer in self.transformers:
            if isinstance(transformer, FieldFilterTransformer):
                steps.append({
                    "type": "filter",
                    "params": {
                        "mode": transformer.mode,
                        "fields": list(transformer.fields)
                    }
                })
            elif isinstance(transformer, RedactionTransformer):
                steps.append({
                    "type": "redaction",
                    "params": {
                        "fields": list(transformer.fields),
                        "replacement": transformer.replacement
                    }
                })
            elif isinstance(transformer, HashingTransformer):
                steps.append({
                    "type": "hashing",
                    "params": {
                        "fields": list(transformer.fields),
                        "salt": transformer.salt.decode() if transformer.salt else ""
                    }
                })
            elif isinstance(transformer, SubstitutionTransformer):
                steps.append({
                    "type": "substitution",
                    "params": {
                        "substitutions": transformer.substitutions
                    }
                })
            else:
                # Custom transformer - use class name
                steps.append({
                    "type": "custom",
                    "params": {
                        "class": transformer.__class__.__name__
                    }
                })

        return {"steps": steps}

    def __len__(self) -> int:
        return len(self.transformers)

    def __repr__(self) -> str:
        return f"TransformerPipeline(transformers={self.transformers})"
