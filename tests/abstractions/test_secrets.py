# Copyright (c) 2025 Scientific Informatics, LLC
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 3.0-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/omopcloudetl_core

import os

import pytest
from omopcloudetl_core.abstractions.secrets import EnvironmentSecretsProvider
from omopcloudetl_core.exceptions import SecretAccessError


@pytest.fixture
def secrets_provider():
    """Fixture to provide an instance of EnvironmentSecretsProvider."""
    return EnvironmentSecretsProvider()


def test_get_secret_from_environment(monkeypatch, secrets_provider):
    """Test retrieving a secret that exists in the environment variables."""
    secret_name = "MY_TEST_SECRET"
    secret_value = "supersecretvalue"
    monkeypatch.setenv(secret_name, secret_value)

    retrieved_value = secrets_provider.get_secret(secret_name)
    assert retrieved_value == secret_value


def test_get_secret_not_found_raises_error(secrets_provider):
    """Test that a SecretAccessError is raised if the environment variable is not set."""
    secret_name = "NON_EXISTENT_SECRET"
    # Ensure the environment variable is not set
    if os.getenv(secret_name):
        del os.environ[secret_name]

    with pytest.raises(SecretAccessError) as excinfo:
        secrets_provider.get_secret(secret_name)

    assert secret_name in str(excinfo.value)
    assert "not found in environment" in str(excinfo.value)


def test_get_secret_with_empty_value(monkeypatch, secrets_provider):
    """Test retrieving a secret that is set to an empty string."""
    secret_name = "EMPTY_SECRET"
    secret_value = ""
    monkeypatch.setenv(secret_name, secret_value)

    retrieved_value = secrets_provider.get_secret(secret_name)
    assert retrieved_value == secret_value
