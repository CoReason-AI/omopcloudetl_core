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
from typing import Optional

import yaml
from pydantic import ValidationError

from omopcloudetl_core.config.models import ProjectConfig
from omopcloudetl_core.discovery import DiscoveryManager
from omopcloudetl_core.exceptions import ConfigurationError


class ConfigManager:
    """Manages loading and validation of the project configuration."""

    def __init__(self, discovery_manager: Optional[DiscoveryManager] = None):
        self._discovery = discovery_manager or DiscoveryManager()

    def load_project_config(self, config_path: Path) -> ProjectConfig:
        """
        Loads the project configuration from a YAML file, validates it,
        and resolves any secrets.

        Args:
            config_path: The path to the project configuration YAML file.

        Returns:
            A validated ProjectConfig object.

        Raises:
            ConfigurationError: If the file cannot be read, is not valid YAML,
                                or fails Pydantic validation.
        """
        try:
            with open(config_path, "r") as f:
                config_data = yaml.safe_load(f)
        except FileNotFoundError:
            raise ConfigurationError(f"Configuration file not found at: {config_path}")
        except yaml.YAMLError as e:
            raise ConfigurationError(f"Invalid YAML in configuration file: {config_path}") from e

        if not isinstance(config_data, dict):
            raise ConfigurationError("The root of the configuration file must be a dictionary.")

        try:
            config = ProjectConfig.model_validate(config_data)
        except ValidationError as e:
            raise ConfigurationError(f"Configuration validation failed: {e}") from e

        # Secret Resolution Logic
        if config.connection.password_secret_id and config.connection.password is None:
            secrets_provider = self._discovery.get_secrets_provider(config.secrets)
            resolved_password = secrets_provider.get_secret(config.connection.password_secret_id)
            # Pydantic models are immutable by default, so we create a copy with the updated value.
            # We must handle the nested structure.
            connection_dict = config.connection.model_dump()
            connection_dict["password"] = resolved_password

            config_dict = config.model_dump()
            config_dict["connection"] = connection_dict

            # Re-validate the model with the resolved password
            config = ProjectConfig.model_validate(config_dict)

        return config
