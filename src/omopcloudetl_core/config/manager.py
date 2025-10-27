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
from pydantic import ValidationError

from omopcloudetl_core.config.models import ProjectConfig
from omopcloudetl_core.exceptions import ConfigurationError


class ConfigManager:
    """Manages the loading and validation of project configuration."""

    def load_project_config(self, config_path: Path) -> ProjectConfig:
        """
        Loads a project configuration file, validates its structure,
        and prepares it for secret resolution.

        Args:
            config_path: The path to the YAML configuration file.

        Returns:
            A validated ProjectConfig object.

        Raises:
            ConfigurationError: If the file is not found, cannot be parsed,
                                or fails Pydantic validation.
        """
        if not config_path.is_file():
            raise ConfigurationError(f"Configuration file not found at: {config_path}")

        try:
            with open(config_path, "r") as f:
                config_data = yaml.safe_load(f)
        except yaml.YAMLError as e:
            raise ConfigurationError(f"Error parsing YAML configuration file: {e}") from e

        if not config_data:
            raise ConfigurationError("Configuration file is empty.")

        try:
            project_config = ProjectConfig(**config_data)
        except (ValidationError, ConfigurationError) as e:
            raise ConfigurationError(f"Configuration validation failed: {e}") from e

        # Resolve secrets. In a future phase, this will use the DiscoveryManager
        # to dynamically load the correct secrets provider. For now, we default
        # to the EnvironmentSecretsProvider as per the specification.
        if (
            project_config.secrets
            and project_config.connection.password_secret_id
            and not project_config.connection.password
        ):
            # Local import to avoid circular dependencies if DiscoveryManager is used here later
            from omopcloudetl_core.abstractions.secrets import EnvironmentSecretsProvider

            # NOTE: This part will be replaced by the DiscoveryManager in Phase 6
            if project_config.secrets.provider_type == "environment":
                provider = EnvironmentSecretsProvider()
            else:
                # In the future, we would raise a DiscoveryError here if the provider is unknown
                # For now, we'll assume the environment provider for simplicity in this phase
                provider = EnvironmentSecretsProvider()

            try:
                resolved_password = provider.get_secret(project_config.connection.password_secret_id)
                # Pydantic models are immutable by default, so we need to create a new object
                # for the update.
                connection_data = project_config.connection.model_dump()
                connection_data["password"] = resolved_password

                # Re-create the connection config and update the project config
                new_connection_config = type(project_config.connection)(**connection_data)
                project_config.connection = new_connection_config

            except Exception as e:
                # Catching a broad exception to handle any secret provider error
                raise ConfigurationError(
                    f"Failed to resolve secret '{project_config.connection.password_secret_id}' "
                    f"using provider '{project_config.secrets.provider_type}': {e}"
                ) from e

        return project_config
