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
from omopcloudetl_core.abstractions.secrets import (
    BaseSecretsProvider,
    EnvironmentSecretsProvider,
)
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


def test_abstract_method_not_implemented():
    class IncompleteSecretsProvider(BaseSecretsProvider):
        pass

    with pytest.raises(TypeError):
        IncompleteSecretsProvider()
