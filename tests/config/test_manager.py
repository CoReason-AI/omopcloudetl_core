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
from omopcloudetl_core.abstractions.secrets import BaseSecretsProvider
from omopcloudetl_core.config.manager import ConfigManager
from omopcloudetl_core.exceptions import ConfigurationError


@pytest.fixture
def mock_discovery_manager():
    """Fixture to create a mock DiscoveryManager."""
    mock_dm = MagicMock()
    mock_secrets_provider = MagicMock(spec=BaseSecretsProvider)
    mock_secrets_provider.get_secret.return_value = "supersecretpassword"
    mock_dm.get_secrets_provider.return_value = mock_secrets_provider
    return mock_dm


def test_load_project_config_success(tmp_path: Path):
    """
    Tests that a valid project configuration is loaded successfully.
    """
    config_content = """
connection:
  provider_type: "duckdb"
orchestrator:
  type: "local"
schemas:
  source: "main"
  target: "main"
"""
    config_file = tmp_path / "project.yml"
    config_file.write_text(config_content)

    manager = ConfigManager()
    config = manager.load_project_config(config_file)

    assert config.connection.provider_type == "duckdb"
    assert config.orchestrator.type == "local"
    assert config.schemas["source"] == "main"


def test_load_project_config_with_secret_resolution(tmp_path: Path, mock_discovery_manager: MagicMock):
    """
    Tests that the password is correctly resolved using a secrets provider.
    """
    config_content = """
connection:
  provider_type: "postgres"
  user: "testuser"
  password_secret_id: "my_db_password"
orchestrator:
  type: "local"
schemas:
  source: "public"
  target: "public"
secrets:
  provider_type: "env"
"""
    config_file = tmp_path / "project.yml"
    config_file.write_text(config_content)

    manager = ConfigManager(discovery_manager=mock_discovery_manager)
    config = manager.load_project_config(config_file)

    assert config.connection.password is not None
    assert config.connection.password.get_secret_value() == "supersecretpassword"
    # Verify that get_secret was called with the correct ID
    mock_discovery_manager.get_secrets_provider.return_value.get_secret.assert_called_once_with("my_db_password")


def test_load_project_config_file_not_found():
    """
    Tests that a ConfigurationError is raised if the config file does not exist.
    """
    manager = ConfigManager()
    with pytest.raises(ConfigurationError, match="Configuration file not found"):
        manager.load_project_config(Path("non_existent_file.yml"))


def test_load_project_config_invalid_yaml(tmp_path: Path):
    """
    Tests that a ConfigurationError is raised for a malformed YAML file.
    """
    config_file = tmp_path / "project.yml"
    config_file.write_text("connection: { provider_type: 'duckdb'") # Malformed YAML
    manager = ConfigManager()
    with pytest.raises(ConfigurationError, match="Error parsing YAML"):
        manager.load_project_config(config_file)


def test_load_project_config_validation_error(tmp_path: Path):
    """
    Tests that a ConfigurationError is raised if the config fails Pydantic validation.
    """
    config_content = """
connection:
  # provider_type is missing, which is a required field
  host: "localhost"
orchestrator:
  type: "local"
schemas:
  source: "main"
"""
    config_file = tmp_path / "project.yml"
    config_file.write_text(config_content)
    manager = ConfigManager()
    with pytest.raises(ConfigurationError, match="Configuration validation failed"):
        manager.load_project_config(config_file)
