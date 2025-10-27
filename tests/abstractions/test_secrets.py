import os
import pytest
from omopcloudetl_core.abstractions.secrets import EnvironmentSecretsProvider
from omopcloudetl_core.exceptions import SecretAccessError


@pytest.fixture
def secrets_provider():
    return EnvironmentSecretsProvider()


def test_get_secret_success(secrets_provider, monkeypatch):
    monkeypatch.setenv("MY_SECRET", "my_value")
    assert secrets_provider.get_secret("MY_SECRET") == "my_value"


def test_get_secret_not_found(secrets_provider):
    with pytest.raises(SecretAccessError, match="Secret not found in environment: NON_EXISTENT_SECRET"):
        secrets_provider.get_secret("NON_EXISTENT_SECRET")
