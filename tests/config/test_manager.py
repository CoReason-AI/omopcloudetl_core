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
from pathlib import Path
from unittest.mock import patch, mock_open
from omopcloudetl_core.config.manager import ConfigManager
from omopcloudetl_core.exceptions import ConfigurationError

@pytest.fixture
def config_manager():
    """Provides a ConfigManager instance."""
    return ConfigManager()

VALID_YAML = """
connection:
  provider_type: 'test'
  host: 'localhost'
orchestrator:
  type: 'local'
schemas:
  source: 'raw'
  target: 'cdm'
"""

SECRET_YAML = """
connection:
  provider_type: 'test'
  password_secret_id: 'MY_DB_PASSWORD'
orchestrator:
  type: 'local'
schemas:
  source: 'raw'
"""

def test_load_project_config_success(config_manager):
    """Tests successfully loading a valid YAML configuration file."""
    with patch("builtins.open", mock_open(read_data=VALID_YAML)) as mock_file:
        with patch("pathlib.Path.is_file", return_value=True):
            config = config_manager.load_project_config(Path("dummy/path/config.yml"))
            assert config.connection.provider_type == "test"
            assert config.schemas["source"] == "raw"
            mock_file.assert_called_once_with(Path("dummy/path/config.yml"), "r")

def test_load_project_config_resolves_secret(config_manager, monkeypatch):
    """Tests that the ConfigManager correctly resolves secrets."""
    secret_key = "MY_DB_PASSWORD"
    secret_value = "password123"
    monkeypatch.setenv(secret_key, secret_value)

    with patch("builtins.open", mock_open(read_data=SECRET_YAML)):
        with patch("pathlib.Path.is_file", return_value=True):
            config = config_manager.load_project_config(Path("config.yml"))
            assert config.connection.password.get_secret_value() == secret_value

def test_load_config_file_not_found(config_manager):
    """Tests that a ConfigurationError is raised for a missing config file."""
    with patch("pathlib.Path.is_file", return_value=False):
        with pytest.raises(ConfigurationError, match="not found"):
            config_manager.load_project_config(Path("non_existent_file.yml"))

def test_load_invalid_yaml(config_manager):
    """Tests that a ConfigurationError is raised for invalid YAML."""
    invalid_yaml = "connection: { provider_type: 'test'"
    with patch("builtins.open", mock_open(read_data=invalid_yaml)):
        with patch("pathlib.Path.is_file", return_value=True):
            with pytest.raises(ConfigurationError):
                config_manager.load_project_config(Path("invalid.yml"))
