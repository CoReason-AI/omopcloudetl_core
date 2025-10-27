# Copyright (c) 2025 Scientific Informatics, LLC
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/omopcloudetl_core


import os
from unittest.mock import patch

import pytest
from pydantic import ValidationError

from omopcloudetl_core.config.models import ConnectionConfig, ProjectConfig
from omopcloudetl_core.exceptions import ConfigurationError


class TestConnectionConfig:
    def test_successful_creation(self):
        config = ConnectionConfig(provider_type="test", host="localhost", user="user")
        assert config.provider_type == "test"
        assert config.host == "localhost"
        assert config.user == "user"

    def test_env_var_override(self):
        with patch.dict(
            os.environ,
            {
                "OMOPCLOUDETL_CONN_HOST": "env_host",
                "OMOPCLOUDETL_CONN_USER": "env_user",
                "OMOPCLOUDETL_CONN_PROVIDER_TYPE": "env_provider",
            },
        ):
            config = ConnectionConfig()
            assert config.provider_type == "env_provider"
            assert config.host == "env_host"
            assert config.user == "env_user"

    def test_password_secretstr(self):
        config = ConnectionConfig(provider_type="test", password="test_password")
        assert config.password.get_secret_value() == "test_password"

    def test_extra_settings(self):
        config = ConnectionConfig(provider_type="test", extra_settings={"key": "value"})
        assert config.extra_settings["key"] == "value"


class TestProjectConfig:
    def test_successful_creation(self):
        config_data = {
            "connection": {"provider_type": "test", "host": "localhost"},
            "orchestrator": {"type": "local", "configuration": {}},
            "schemas": {"source": "raw", "target": "cdm"},
            "secrets": {"provider_type": "env", "configuration": {}},
        }
        config = ProjectConfig(**config_data)
        assert config.connection.host == "localhost"
        assert config.orchestrator.type == "local"
        assert config.schemas["source"] == "raw"
        assert config.secrets.provider_type == "env"

    def test_missing_required_fields(self):
        with pytest.raises(ValidationError):
            ProjectConfig(
                connection={"provider_type": "test"},
                orchestrator={"type": "local", "configuration": {}},
            )

    def test_optional_secrets(self):
        config_data = {
            "connection": {"provider_type": "test"},
            "orchestrator": {"type": "local"},
            "schemas": {},
        }
        config = ProjectConfig(**config_data)
        assert config.secrets is None

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
            ConfigurationError,
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
            "secrets": {"provider_type": "env"},
        }
        # Should not raise
        ProjectConfig(**config_data)

    def test_password_secret_id_with_direct_password_is_valid(self):
        """
        Tests that the validator does not raise an error if a direct password
        is also provided, even if the secrets config is missing.
        """
        config_data = {
            "connection": {"provider_type": "test", "password_secret_id": "my-secret", "password": "direct-password"},
            "orchestrator": {"type": "local"},
            "schemas": {},
        }
        # Should not raise
        ProjectConfig(**config_data)
