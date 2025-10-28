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
from unittest import mock

from pydantic import ValidationError, SecretStr
import pytest
from omopcloudetl_core.config.models import ConnectionConfig, ProjectConfig, SecretsConfig


def test_project_config_creation():
    """Tests the successful creation of a ProjectConfig model."""
    config_data = {
        "connection": {"provider_type": "test_provider"},
        "orchestrator": {"type": "test_orchestrator", "configuration": {}},
        "schemas": {"source": "raw", "target": "cdm"},
    }
    project_config = ProjectConfig(**config_data)
    assert project_config.connection.provider_type == "test_provider"
    assert project_config.orchestrator.type == "test_orchestrator"
    assert project_config.schemas["source"] == "raw"


def test_project_config_missing_fields():
    """Tests that ProjectConfig raises a validation error for missing required fields."""
    with pytest.raises(ValidationError):
        ProjectConfig(
            connection={"provider_type": "test_provider"},
            schemas={"source": "raw"},
        )


def test_connection_config_env_vars(monkeypatch):
    """Tests that ConnectionConfig correctly loads settings from environment variables."""
    monkeypatch.setenv("OMOPCLOUDETL_CONN_HOST", "db.example.com")
    monkeypatch.setenv("OMOPCLOUDETL_CONN_USER", "testuser")
    monkeypatch.setenv("OMOPCLOUDETL_CONN_PASSWORD", "supersecret")

    conn_config = ConnectionConfig(provider_type="test_provider")
    assert conn_config.host == "db.example.com"
    assert conn_config.user == "testuser"
    assert conn_config.password.get_secret_value() == "supersecret"
    assert isinstance(conn_config.password, SecretStr)


def test_secrets_config_creation():
    """Tests the successful creation of a SecretsConfig model."""
    secrets_data = {
        "provider_type": "env",
        "configuration": {"prefix": "MY_APP_"},
    }
    secrets_config = SecretsConfig(**secrets_data)
    assert secrets_config.provider_type == "env"
    assert secrets_config.configuration["prefix"] == "MY_APP_"

