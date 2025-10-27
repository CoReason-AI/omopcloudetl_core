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
import yaml
from unittest.mock import patch

from omopcloudetl_core.config.manager import ConfigManager
from omopcloudetl_core.exceptions import ConfigurationError


@pytest.fixture
def config_manager():
    return ConfigManager()


def test_load_project_config_success(config_manager, tmp_path: Path):
    """
    Tests successful loading of a valid project configuration file.
    """
    config_content = {
        "connection": {"provider_type": "test_db"},
        "orchestrator": {"type": "local"},
        "schemas": {"source": "raw", "target": "cdm"},
    }
    config_file = tmp_path / "config.yml"
    with open(config_file, "w") as f:
        yaml.dump(config_content, f)

    project_config = config_manager.load_project_config(config_file)
    assert project_config.connection.provider_type == "test_db"
    assert project_config.orchestrator.type == "local"


def test_load_config_file_not_found(config_manager):
    """
    Tests that a ConfigurationError is raised when the config file does not exist.
    """
    non_existent_path = Path("non_existent_config.yml")
    with pytest.raises(ConfigurationError, match="Configuration file not found"):
        config_manager.load_project_config(non_existent_path)


def test_load_config_invalid_yaml(config_manager, tmp_path: Path):
    """
    Tests that a ConfigurationError is raised for a file with invalid YAML syntax.
    """
    config_file = tmp_path / "invalid.yml"
    config_file.write_text("connection: { provider_type: test_db")  # Malformed YAML
    with pytest.raises(ConfigurationError, match="Error parsing YAML"):
        config_manager.load_project_config(config_file)


def test_load_config_empty_file(config_manager, tmp_path: Path):
    """
    Tests that a ConfigurationError is raised for an empty configuration file.
    """
    config_file = tmp_path / "empty.yml"
    config_file.touch()
    with pytest.raises(ConfigurationError, match="Configuration file is empty"):
        config_manager.load_project_config(config_file)


def test_load_config_validation_error(config_manager, tmp_path: Path):
    """
    Tests that a ConfigurationError is raised if the configuration
    is missing required fields (fails Pydantic validation).
    """
    config_content = {
        "orchestrator": {"type": "local"},
        "schemas": {"source": "raw", "target": "cdm"},
    }  # Missing 'connection'
    config_file = tmp_path / "invalid_schema.yml"
    with open(config_file, "w") as f:
        yaml.dump(config_content, f)

    with pytest.raises(ConfigurationError, match="Configuration validation failed"):
        config_manager.load_project_config(config_file)


def test_load_config_with_secret_resolution(config_manager, tmp_path: Path):
    """
    Tests that the ConfigManager correctly resolves a password from a secret_id
    using the EnvironmentSecretsProvider.
    """
    secret_key = "DB_PASSWORD_SECRET"
    secret_value = "supersecretpassword"

    config_content = {
        "connection": {"provider_type": "test_db", "password_secret_id": secret_key},
        "orchestrator": {"type": "local"},
        "schemas": {"source": "raw", "target": "cdm"},
        "secrets": {"provider_type": "environment"},
    }
    config_file = tmp_path / "config_with_secret.yml"
    with open(config_file, "w") as f:
        yaml.dump(config_content, f)

    with patch.dict("os.environ", {secret_key: secret_value}):
        project_config = config_manager.load_project_config(config_file)

    assert project_config.connection.password_secret_id == secret_key
    assert project_config.secrets.provider_type == "environment"
    assert project_config.connection.password is not None
    assert project_config.connection.password.get_secret_value() == secret_value


def test_load_config_secret_resolution_fails(config_manager, tmp_path: Path):
    """
    Tests that a ConfigurationError is raised if secret resolution fails.
    """
    config_content = {
        "connection": {"provider_type": "test_db", "password_secret_id": "NON_EXISTENT_SECRET"},
        "orchestrator": {"type": "local"},
        "schemas": {"source": "raw", "target": "cdm"},
        "secrets": {"provider_type": "environment"},
    }
    config_file = tmp_path / "config_with_failing_secret.yml"
    with open(config_file, "w") as f:
        yaml.dump(config_content, f)

    with pytest.raises(ConfigurationError, match="Failed to resolve secret"):
        config_manager.load_project_config(config_file)


def test_load_config_unsupported_secret_provider(config_manager, tmp_path: Path):
    """
    Tests that a ConfigurationError is raised for an unsupported secret provider.
    """
    config_content = {
        "connection": {"provider_type": "test_db", "password_secret_id": "some-secret"},
        "orchestrator": {"type": "local"},
        "schemas": {"source": "raw", "target": "cdm"},
        "secrets": {"provider_type": "unsupported_provider"},
    }
    config_file = tmp_path / "config_with_unsupported_secret_provider.yml"
    with open(config_file, "w") as f:
        yaml.dump(config_content, f)

    with pytest.raises(ConfigurationError, match="is not supported"):
        config_manager.load_project_config(config_file)


def test_load_config_valid_yaml_not_dict(config_manager, tmp_path: Path):
    """
    Tests that a ConfigurationError is raised if the YAML is valid but not a dictionary.
    """
    config_file = tmp_path / "not_a_dict.yml"
    config_file.write_text("- item1\n- item2")  # A list, not a dictionary
    with pytest.raises(ConfigurationError, match="Configuration validation failed"):
        config_manager.load_project_config(config_file)


def test_load_config_prefers_direct_password_over_secret(config_manager, tmp_path: Path, mocker):
    """
    Tests that a direct password in the config is used even if a secret_id is also present.
    """
    direct_password = "direct_password"
    secret_key = "DB_PASSWORD_SECRET"
    secret_value = "supersecretpassword"

    config_content = {
        "connection": {
            "provider_type": "test_db",
            "password": direct_password,
            "password_secret_id": secret_key,
        },
        "orchestrator": {"type": "local"},
        "schemas": {"source": "raw", "target": "cdm"},
        "secrets": {"provider_type": "environment"},
    }
    config_file = tmp_path / "config_with_both_passwords.yml"
    with open(config_file, "w") as f:
        yaml.dump(config_content, f)

    # Mock the secrets provider to ensure it's not called
    mock_get_secret = mocker.patch(
        "omopcloudetl_core.abstractions.secrets.EnvironmentSecretsProvider.get_secret"
    )

    with patch.dict("os.environ", {secret_key: secret_value}):
        project_config = config_manager.load_project_config(config_file)

    assert project_config.connection.password is not None
    assert project_config.connection.password.get_secret_value() == direct_password
    mock_get_secret.assert_not_called()


def test_load_config_with_secrets_block_but_no_secret_id(config_manager, tmp_path: Path, mocker):
    """
    Tests that secret resolution is not triggered if a secrets block is present
    but the connection has no password_secret_id.
    """
    config_content = {
        "connection": {"provider_type": "test_db"},  # No password or secret_id
        "orchestrator": {"type": "local"},
        "schemas": {"source": "raw", "target": "cdm"},
        "secrets": {"provider_type": "environment"},
    }
    config_file = tmp_path / "config.yml"
    with open(config_file, "w") as f:
        yaml.dump(config_content, f)

    mock_get_secret = mocker.patch(
        "omopcloudetl_core.abstractions.secrets.EnvironmentSecretsProvider.get_secret"
    )

    project_config = config_manager.load_project_config(config_file)

    assert project_config.connection.password is None
    mock_get_secret.assert_not_called()


def test_load_config_secret_id_without_secrets_block(config_manager, tmp_path: Path):
    """
    Tests that a ConfigurationError is raised if password_secret_id is provided
    without a secrets configuration block.
    """
    config_content = {
        "connection": {"provider_type": "test_db", "password_secret_id": "some-secret"},
        "orchestrator": {"type": "local"},
        "schemas": {"source": "raw", "target": "cdm"},
        # No 'secrets' block
    }
    config_file = tmp_path / "config.yml"
    with open(config_file, "w") as f:
        yaml.dump(config_content, f)

    with pytest.raises(ConfigurationError, match="Configuration validation failed"):
        config_manager.load_project_config(config_file)
