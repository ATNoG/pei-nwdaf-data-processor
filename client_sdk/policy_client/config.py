"""
Configuration for Policy Client SDK.
"""
import os
from typing import Optional
from dataclasses import dataclass


@dataclass
class ClientConfig:
    """Configuration for Policy Client."""

    # Policy service URL
    POLICY_SERVICE_URL: str = "http://localhost:8000"

    # Component identification
    COMPONENT_ID: str = "unknown_component"

    # Policy enforcement
    ENABLE_POLICY: bool = False

    # Caching
    CACHE_TTL: int = 60
    CACHE_SIZE: int = 1000

    # Request timeout
    TIMEOUT: int = 5

    # Fail-open mode
    FAIL_OPEN: bool = True

    @classmethod
    def from_env(cls) -> "ClientConfig":
        """
        Create configuration from environment variables.

        Environment variables:
            POLICY_SERVICE_URL: URL of the Policy Service
            POLICY_COMPONENT_ID: This component's ID
            POLICY_ENABLED: Whether policy is enabled ("true" or "false")
            POLICY_CACHE_TTL: Cache TTL in seconds
            POLICY_CACHE_SIZE: Maximum cache size
            POLICY_TIMEOUT: Request timeout in seconds
            POLICY_FAIL_OPEN: Whether to fail-open on errors ("true" or "false")

        Returns:
            ClientConfig instance
        """
        return cls(
            POLICY_SERVICE_URL=os.getenv("POLICY_SERVICE_URL", "http://localhost:8000"),
            COMPONENT_ID=os.getenv("POLICY_COMPONENT_ID", "unknown_component"),
            ENABLE_POLICY=os.getenv("POLICY_ENABLED", "false").lower() == "true",
            CACHE_TTL=int(os.getenv("POLICY_CACHE_TTL", "60")),
            CACHE_SIZE=int(os.getenv("POLICY_CACHE_SIZE", "1000")),
            TIMEOUT=int(os.getenv("POLICY_TIMEOUT", "5")),
            FAIL_OPEN=os.getenv("POLICY_FAIL_OPEN", "true").lower() == "true",
        )
