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
from pydantic import ValidationError
from omopcloudetl_core.config.models import ConnectionConfig, ProjectConfig


def test_connection_config_env_vars():
    """Tests that ConnectionConfig correctly loads values from environment variables."""
    with patch.dict(
        "os.environ",
        {
            "OMOPCLOUDETL_CONN_PROVIDER_TYPE": "test_provider",
            "OMOPCLOUDETL_CONN_HOST": "localhost",
            "OMOPCLOUDETL_CONN_USER": "test_user",
        },
    ):
        config = ConnectionConfig()
        assert config.provider_type == "test_provider"
        assert config.host == "localhost"
        assert config.user == "test_user"


class TestProjectConfig:
    """Test suite for the ProjectConfig model."""

    def test_successful_validation(self):
        """Tests that a valid ProjectConfig model is successfully validated."""
        config_data = {
            "connection": {"provider_type": "test"},
            "orchestrator": {"type": "local"},
            "schemas": {"source": "my_schema"},
        }
        ProjectConfig(**config_data)

    def test_missing_connection_raises_error(self):
        """Tests that a missing 'connection' block raises a validation error."""
        config_data = {
            "orchestrator": {"type": "local"},
            "schemas": {},
        }
        with pytest.raises(ValidationError):
            ProjectConfig(**config_data)

    def test_password_secret_id_without_secrets_config_raises_error(self):
        """
        Tests the validator that ensures if a password_secret_id is given,
        a secrets provider must also be configured.
        """
        config_data = {
            "connection": {"provider_type": "test", "password_secret_id": "my-secret"},
            "orchestrator": {"type": "local"},
            "schemas": {},
        }
        with pytest.raises(
            ValidationError,
            match="A 'secrets' provider must be configured when 'connection.password_secret_id' is used.",
        ):
            ProjectConfig(**config_data)

    def test_password_secret_id_with_secrets_config_is_valid(self):
        """
        Tests that the configuration is valid when both password_secret_id
        and a secrets provider are configured.
        """
        config_data = {
            "connection": {"provider_type": "test", "password_secret_id": "my-secret"},
            "orchestrator": {"type": "local"},
            "schemas": {},
            "secrets": {"provider_type": "environment"},
        }
        ProjectConfig(**config_data)
