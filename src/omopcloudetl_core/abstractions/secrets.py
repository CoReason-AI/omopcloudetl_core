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
    @abstractmethod
    def get_secret(self, secret_identifier: str) -> str:
        pass

# Default implementation
class EnvironmentSecretsProvider(BaseSecretsProvider):
    def get_secret(self, secret_identifier: str) -> str:
        value = os.getenv(secret_identifier)
        if value is None:
            # Import locally to avoid circular dependency
            from ..exceptions import SecretAccessError
            raise SecretAccessError(f"Secret not found in environment: {secret_identifier}")
        return value
