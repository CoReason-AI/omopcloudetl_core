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
from omopcloudetl_core.exceptions import ConfigurationError, SecretAccessError

VALID_YAML = """
connection:
  provider_type: "test"
orchestrator:
  type: "local"
schemas:
  source: "raw"
  target: "cdm"
"""

INVALID_YAML = "this is not a dictionary"

YAML_WITH_SECRET = """
connection:
  provider_type: "test"
  password_secret_id: "DB_PASSWORD"
orchestrator:
  type: "local"
schemas:
  source: "raw"
  target: "cdm"
secrets:
  provider_type: "environment"
"""


@pytest.fixture
def manager():
    return ConfigManager()


def test_load_config_success(manager):
    with patch("builtins.open", mock_open(read_data=VALID_YAML)):
        with patch.object(Path, "is_file", return_value=True):
            config = manager.load_project_config(Path("dummy/path/config.yml"))
    assert config.connection.provider_type == "test"
    assert config.orchestrator.type == "local"
    assert config.schemas["source"] == "raw"


def test_load_config_not_found(manager):
    with patch.object(Path, "is_file", return_value=False):
        with pytest.raises(ConfigurationError, match="Configuration file not found"):
            manager.load_project_config(Path("non_existent_file.yml"))


def test_load_config_invalid_yaml(manager):
    with patch("builtins.open", mock_open(read_data=INVALID_YAML)):
        with patch.object(Path, "is_file", return_value=True):
            with pytest.raises(ConfigurationError, match="Failed to load or validate configuration"):
                manager.load_project_config(Path("dummy/path/invalid.yml"))


def test_secret_resolution_success(manager):
    with patch("builtins.open", mock_open(read_data=YAML_WITH_SECRET)):
        with patch.object(Path, "is_file", return_value=True):
            with patch.dict("os.environ", {"DB_PASSWORD": "supersecret"}):
                config = manager.load_project_config(Path("dummy/path/secret.yml"))
    assert config.connection.password.get_secret_value() == "supersecret"


def test_secret_resolution_failure(manager):
    with patch("builtins.open", mock_open(read_data=YAML_WITH_SECRET)):
        with patch.object(Path, "is_file", return_value=True):
            with patch.dict("os.environ", {}, clear=True):
                with pytest.raises(SecretAccessError):
                    manager.load_project_config(Path("dummy/path/secret.yml"))
