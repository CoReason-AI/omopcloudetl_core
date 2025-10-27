# Copyright (c) 2025 Scientific Informatics, LLC
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/omopcloudetl_core

from pathlib import Path
from unittest.mock import MagicMock

import pytest
import yaml
from omopcloudetl_core.config.manager import ConfigManager
from omopcloudetl_core.exceptions import ConfigurationError, SecretAccessError


@pytest.fixture
def mock_discovery_manager():
    """Fixture to create a mock DiscoveryManager."""
    return MagicMock()


@pytest.fixture
def config_file_factory(tmp_path):
    """Factory fixture to create temporary config files."""

    def _create_config_file(content):
        path = tmp_path / "config.yml"
        if isinstance(content, dict):
            path.write_text(yaml.dump(content))
        else:
            path.write_text(content)
        return path

    return _create_config_file


VALID_CONFIG_DICT = {
    "connection": {"provider_type": "test_db"},
    "orchestrator": {"type": "local"},
    "schemas": {"source": "raw", "target": "cdm"},
}


def test_load_project_config_success(config_file_factory):
    """Test loading a valid project configuration file."""
    config_path = config_file_factory(VALID_CONFIG_DICT)
    manager = ConfigManager()
    config = manager.load_project_config(config_path)

    assert config.connection.provider_type == "test_db"
    assert config.orchestrator.type == "local"
    assert config.schemas["target"] == "cdm"


def test_load_config_file_not_found():
    """Test that ConfigurationError is raised for a non-existent file."""
    manager = ConfigManager()
    with pytest.raises(ConfigurationError, match="not found"):
        manager.load_project_config(Path("non_existent_file.yml"))


def test_load_config_invalid_yaml(config_file_factory):
    """Test that ConfigurationError is raised for an invalid YAML file."""
    # This YAML is syntactically incorrect and will cause a YAMLError
    config_path = config_file_factory("key: 'unclosed quote")
    manager = ConfigManager()
    with pytest.raises(ConfigurationError, match="Invalid YAML"):
        manager.load_project_config(config_path)


def test_load_config_validation_error(config_file_factory):
    """Test that ConfigurationError is raised for a config that fails Pydantic validation."""
    invalid_config = {"connection": {"provider_type": "test"}}  # Missing 'orchestrator' and 'schemas'
    config_path = config_file_factory(invalid_config)
    manager = ConfigManager()
    with pytest.raises(ConfigurationError, match="Configuration validation failed"):
        manager.load_project_config(config_path)


def test_secret_resolution_success(config_file_factory, mock_discovery_manager):
    """Test successful resolution of a password from a secret."""
    config_with_secret = {
        **VALID_CONFIG_DICT,
        "connection": {
            "provider_type": "test_db",
            "password_secret_id": "my-db-password",
        },
        "secrets": {"provider_type": "env"},
    }
    config_path = config_file_factory(config_with_secret)

    # Configure the mock secrets provider
    mock_provider = MagicMock()
    mock_provider.get_secret.return_value = "resolved_secret_password"
    mock_discovery_manager.get_secrets_provider.return_value = mock_provider

    manager = ConfigManager(discovery_manager=mock_discovery_manager)
    config = manager.load_project_config(config_path)

    mock_discovery_manager.get_secrets_provider.assert_called_once()
    mock_provider.get_secret.assert_called_with("my-db-password")
    assert config.connection.password.get_secret_value() == "resolved_secret_password"


def test_secret_resolution_fails(config_file_factory, mock_discovery_manager):
    """Test that an exception from the secrets provider is propagated."""
    config_with_secret = {
        **VALID_CONFIG_DICT,
        "connection": {
            "provider_type": "test_db",
            "password_secret_id": "my-db-password",
        },
        "secrets": {"provider_type": "env"},
    }
    config_path = config_file_factory(config_with_secret)

    mock_provider = MagicMock()
    mock_provider.get_secret.side_effect = SecretAccessError("Secret not found")
    mock_discovery_manager.get_secrets_provider.return_value = mock_provider

    manager = ConfigManager(discovery_manager=mock_discovery_manager)
    with pytest.raises(SecretAccessError):
        manager.load_project_config(config_path)


def test_no_secret_resolution_if_password_present(config_file_factory, mock_discovery_manager):
    """Test that secret resolution is skipped if a password is provided directly."""
    config_with_direct_pass = {
        **VALID_CONFIG_DICT,
        "connection": {
            "provider_type": "test_db",
            "password": "direct_password",
            "password_secret_id": "should_be_ignored",
        },
    }
    config_path = config_file_factory(config_with_direct_pass)
    manager = ConfigManager(discovery_manager=mock_discovery_manager)
    config = manager.load_project_config(config_path)

    mock_discovery_manager.get_secrets_provider.assert_not_called()
    assert config.connection.password.get_secret_value() == "direct_password"
