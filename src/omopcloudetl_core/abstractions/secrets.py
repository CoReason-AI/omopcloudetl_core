# Copyright (c) 2025 Scientific Informatics, LLC
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/omopcloudetl_core

from abc import ABC, abstractmethod
import os


class BaseSecretsProvider(ABC):
    """Abstract base class for a secrets provider."""

    @abstractmethod
    def get_secret(self, secret_identifier: str) -> str:  # pragma: no cover
        """
        Retrieves a secret value for a given identifier.

        Args:
            secret_identifier: The identifier for the secret to retrieve.

        Returns:
            The secret value as a string.
        """
        pass


# Default implementation
class EnvironmentSecretsProvider(BaseSecretsProvider):
    """A secrets provider that retrieves secrets from environment variables."""

    def get_secret(self, secret_identifier: str) -> str:
        """
        Retrieves a secret from an environment variable.

        Args:
            secret_identifier: The name of the environment variable.

        Returns:
            The value of the environment variable.

        Raises:
            SecretAccessError: If the environment variable is not set.
        """
        value = os.getenv(secret_identifier)
        if value is None:
            # Import locally to avoid circular dependency
            from ..exceptions import SecretAccessError

            raise SecretAccessError(f"Secret not found in environment: {secret_identifier}")
        return value
