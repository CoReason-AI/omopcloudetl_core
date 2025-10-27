from abc import ABC, abstractmethod
import os


class BaseSecretsProvider(ABC):
    @abstractmethod
    def get_secret(self, secret_identifier: str) -> str:
        pass


class EnvironmentSecretsProvider(BaseSecretsProvider):
    def get_secret(self, secret_identifier: str) -> str:
        value = os.getenv(secret_identifier)
        if value is None:
            # Import locally to avoid circular dependency
            from ..exceptions import SecretAccessError

            raise SecretAccessError(f"Secret not found in environment: {secret_identifier}")
        return value
