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
from omopcloudetl_core.config.models import (
    ConnectionConfig,
    ProjectConfig,
    SecretsConfig,
    OrchestratorConfig,
)
from omopcloudetl_core.exceptions import ConfigurationError


def test_connection_config_env_vars(monkeypatch):
    """Test that ConnectionConfig correctly loads settings from environment variables."""
    monkeypatch.setenv("OMOPCLOUDETL_CONN_PROVIDER_TYPE", "test_provider")
    monkeypatch.setenv("OMOPCLOUDETL_CONN_HOST", "testhost")
    monkeypatch.setenv("OMOPCLOUDETL_CONN_USER", "testuser")
    monkeypatch.setenv("OMOPCLOUDETL_CONN_PASSWORD", "testpass")

    config = ConnectionConfig()

    assert config.provider_type == "test_provider"
    assert config.host == "testhost"
    assert config.user == "testuser"
    assert config.password.get_secret_value() == "testpass"


def test_project_config_valid():
    """Test a valid ProjectConfig model passes validation."""
    config_data = {
        "connection": {"provider_type": "test"},
        "orchestrator": {"type": "local"},
        "schemas": {"source": "my_source_schema"},
    }
    project_config = ProjectConfig(**config_data)
    assert project_config.connection.provider_type == "test"
    assert project_config.orchestrator.type == "local"
    assert project_config.schemas["source"] == "my_source_schema"


def test_project_config_password_secret_with_secrets_provider():
    """
    Test validation passes when password_secret_id is used and a secrets
    provider is configured.
    """
    config_data = {
        "connection": {
            "provider_type": "test",
            "password_secret_id": "my_password_secret",
        },
        "orchestrator": {"type": "local"},
        "schemas": {"default": "dbo"},
        "secrets": {"provider_type": "env"},
    }
    # Should not raise ConfigurationError
    ProjectConfig(**config_data)


def test_project_config_password_secret_without_secrets_provider_fails():
    """
    Test that validation fails if password_secret_id is used without a
    secrets provider config.
    """
    config_data = {
        "connection": {
            "provider_type": "test",
            "password_secret_id": "my_password_secret",
        },
        "orchestrator": {"type": "local"},
        "schemas": {"default": "dbo"},
    }
    with pytest.raises(ConfigurationError) as excinfo:
        ProjectConfig(**config_data)

    assert "A 'secrets' provider must be configured" in str(excinfo.value)


def test_project_config_direct_password_no_secrets_needed():
    """
    Test validation passes if a direct password is given, even if a
    password_secret_id is also present (direct password takes precedence).
    """
    config_data = {
        "connection": {
            "provider_type": "test",
            "password": "direct_password",
            "password_secret_id": "this_should_be_ignored",
        },
        "orchestrator": {"type": "local"},
        "schemas": {"default": "dbo"},
        # No secrets provider, which is okay because the password is provided directly
    }
    # Should not raise ConfigurationError
    ProjectConfig(**config_data)


def test_secrets_and_orchestrator_config():
    """Test basic instantiation of SecretsConfig and OrchestratorConfig."""
    secrets_conf = SecretsConfig(provider_type="aws_secrets_manager", configuration={"region": "us-east-1"})
    assert secrets_conf.provider_type == "aws_secrets_manager"
    assert secrets_conf.configuration["region"] == "us-east-1"

    orchestrator_conf = OrchestratorConfig(type="airflow", configuration={"dag_id": "my_etl"})
    assert orchestrator_conf.type == "airflow"
    assert orchestrator_conf.configuration["dag_id"] == "my_etl"
