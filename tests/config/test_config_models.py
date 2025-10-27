import os
from unittest.mock import patch

import pytest
from pydantic import ValidationError

from omopcloudetl_core.config.models import ConnectionConfig, ProjectConfig


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
