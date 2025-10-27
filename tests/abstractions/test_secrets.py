# Copyright (c) 2025 Scientific Informatics, LLC
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 3.0-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/omopcloudetl_core

import pytest
from unittest.mock import patch
from omopcloudetl_core.abstractions.secrets import EnvironmentSecretsProvider
from omopcloudetl_core.exceptions import SecretAccessError

def test_get_secret_success():
    provider = EnvironmentSecretsProvider()
    secret_key = "MY_TEST_SECRET"
    secret_value = "my_secret_value"
    with patch.dict("os.environ", {secret_key: secret_value}):
        assert provider.get_secret(secret_key) == secret_value

def test_get_secret_not_found():
    provider = EnvironmentSecretsProvider()
    secret_key = "NON_EXISTENT_SECRET"
    with patch.dict("os.environ", {}, clear=True):
        with pytest.raises(SecretAccessError, match=f"Secret not found in environment: {secret_key}"):
            provider.get_secret(secret_key)
