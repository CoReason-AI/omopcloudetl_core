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
from pydantic import ValidationError, SecretStr
from omopcloudetl_core.config.models import (
    ConnectionConfig,
    ProjectConfig,
    SecretsConfig,
    OrchestratorConfig,
)

def test_connection_config_loads_from_env(monkeypatch):
    """Tests that ConnectionConfig correctly loads settings from environment variables."""
    monkeypatch.setenv("OMOPCLOUDETL_CONN_PROVIDER_TYPE", "test_provider")
    monkeypatch.setenv("OMOPCLOUDETL_CONN_HOST", "local.test")
    monkeypatch.setenv("OMOPCLOUDETL_CONN_USER", "test_user")

    config = ConnectionConfig()
    assert config.provider_type == "test_provider"
    assert config.host == "local.test"
    assert config.user == "test_user"

def test_connection_config_password_is_secret():
    """Tests that the 'password' field is treated as a Pydantic SecretStr."""
    password_value = "mysecret"
    config = ConnectionConfig(provider_type="test", password=password_value)
    assert isinstance(config.password, SecretStr)
    assert config.password.get_secret_value() == password_value

def test_project_config_validation():
    """Tests the successful validation of a complete ProjectConfig model."""
    config_data = {
        "connection": {"provider_type": "test_db"},
        "orchestrator": {"type": "local_test"},
        "schemas": {"source": "raw", "target": "cdm"},
        "secrets": {"provider_type": "env"},
    }
    project_config = ProjectConfig.model_validate(config_data)
    assert isinstance(project_config.connection, ConnectionConfig)
    assert isinstance(project_config.orchestrator, OrchestratorConfig)
    assert isinstance(project_config.secrets, SecretsConfig)
    assert project_config.schemas["target"] == "cdm"

def test_project_config_missing_required_fields():
    """Tests that validation fails if required fields are missing."""
    with pytest.raises(ValidationError):
        ProjectConfig.model_validate({"connection": {"provider_type": "test"}})

    with pytest.raises(ValidationError):
        ProjectConfig.model_validate({"orchestrator": {"type": "test"}})
