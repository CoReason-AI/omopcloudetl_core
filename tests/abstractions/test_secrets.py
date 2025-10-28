# Copyright (c) 2025 Scientific Informatics, LLC
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/omopcloudetl_core

import pytest
from unittest.mock import patch
from omopcloudetl_core.abstractions.secrets import EnvironmentSecretsProvider
from omopcloudetl_core.exceptions import SecretAccessError


@pytest.fixture
def secrets_provider():
    """Fixture to provide an instance of the EnvironmentSecretsProvider."""
    return EnvironmentSecretsProvider()


def test_get_secret_success(secrets_provider, monkeypatch):
    """Tests successfully retrieving a secret from an environment variable."""
    secret_key = "MY_TEST_SECRET"
    secret_value = "supersecretvalue"
    monkeypatch.setenv(secret_key, secret_value)

    assert secrets_provider.get_secret(secret_key) == secret_value


def test_get_secret_not_found(secrets_provider, monkeypatch):
    """Tests that a SecretAccessError is raised when the secret is not found."""
    secret_key = "NON_EXISTENT_SECRET"
    # Ensure the environment variable is not set
    monkeypatch.delenv(secret_key, raising=False)

    with pytest.raises(SecretAccessError) as excinfo:
        secrets_provider.get_secret(secret_key)

    assert secret_key in str(excinfo.value)
    assert "Secret not found" in str(excinfo.value)


@patch("os.getenv")
def test_get_secret_with_mock(mock_getenv, secrets_provider):
    """Tests the provider by mocking os.getenv."""
    secret_key = "MOCKED_SECRET"
    secret_value = "mocked_value"
    mock_getenv.return_value = secret_value

    result = secrets_provider.get_secret(secret_key)

    mock_getenv.assert_called_once_with(secret_key)
    assert result == secret_value
