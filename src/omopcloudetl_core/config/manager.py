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
from pydantic import SecretStr, ValidationError

from ..discovery import DiscoveryManager
from ..exceptions import ConfigurationError
from .models import ProjectConfig


class ConfigManager:
    """
    Manages the loading and resolution of the project configuration.
    """

    def __init__(self, discovery_manager: DiscoveryManager | None = None):
        self._discovery = discovery_manager or DiscoveryManager()

    def load_project_config(self, config_path: Path) -> ProjectConfig:
        """
        Loads the project configuration from a YAML file, resolves any secrets,
        and returns a validated ProjectConfig object.

        Args:
            config_path: The path to the project configuration YAML file.

        Returns:
            A populated and validated ProjectConfig object.

        Raises:
            ConfigurationError: If the file is not found, cannot be parsed,
                                or fails validation.
        """
        if not config_path.exists():
            raise ConfigurationError(f"Configuration file not found at: {config_path}")

        try:
            with open(config_path, "r") as f:
                config_data = yaml.safe_load(f)
        except yaml.YAMLError as e:
            raise ConfigurationError(f"Error parsing YAML configuration file: {e}") from e

        try:
            config = ProjectConfig.model_validate(config_data)
        except ValidationError as e:
            raise ConfigurationError(f"Configuration validation failed: {e}") from e

        # HLD Mandate: Secret Resolution Logic
        if config.secrets and config.connection.password_secret_id:
            if not config.connection.password:
                secrets_provider = self._discovery.get_secrets_provider(config.secrets)
                resolved_password = secrets_provider.get_secret(
                    config.connection.password_secret_id
                )
                config.connection.password = SecretStr(resolved_password)

        return config
