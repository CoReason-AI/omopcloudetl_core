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
import yaml
from pydantic import ValidationError, SecretStr
from omopcloudetl_core.config.models import ProjectConfig
from omopcloudetl_core.exceptions import ConfigurationError
from omopcloudetl_core.discovery import DiscoveryManager


class ConfigManager:
    """Manages the loading and validation of project configuration."""

    def load_project_config(self, config_path: Path) -> ProjectConfig:
        """
        Loads, validates, and resolves secrets for the project configuration.

        Args:
            config_path: The path to the project's YAML configuration file.

        Returns:
            A validated ProjectConfig object.

        Raises:
            ConfigurationError: If the configuration file is not found, invalid,
                                or fails validation.
        """
        if not config_path.is_file():
            raise ConfigurationError(f"Configuration file not found at: {config_path}")

        try:
            with open(config_path, "r") as f:
                config_data = yaml.safe_load(f)

            if not isinstance(config_data, dict):
                raise TypeError("Configuration content is not a valid dictionary.")

            config = ProjectConfig.model_validate(config_data)
        except (yaml.YAMLError, ValidationError, TypeError) as e:
            raise ConfigurationError(f"Failed to load or validate configuration: {e}") from e

        # Secret Resolution Logic
        if config.connection.password_secret_id and not config.connection.password:
            discovery = DiscoveryManager()
            secrets_provider = discovery.get_secrets_provider(config.secrets)
            resolved_password = secrets_provider.get_secret(config.connection.password_secret_id)
            config.connection.password = SecretStr(resolved_password)

        return config
