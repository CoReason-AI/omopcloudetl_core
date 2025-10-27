# Copyright (c) 2025 Scientific Informatics, LLC
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/omopcloudetl_core

from pydantic import BaseModel, SecretStr, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict
from typing import Optional, Dict, Any

from omopcloudetl_core.exceptions import ConfigurationError


class SecretsConfig(BaseModel):
    """Configuration for the secrets provider."""
    provider_type: str
    configuration: Dict[str, Any] = {}


class OrchestratorConfig(BaseModel):
    """Configuration for the orchestrator."""
    type: str
    configuration: Dict[str, Any] = {}


class ConnectionConfig(BaseSettings):
    """Configuration for the database connection."""
    provider_type: str
    host: Optional[str] = None
    user: Optional[str] = None
    # Password can be set via env var, direct value, or secret_id lookup
    password: Optional[SecretStr] = None
    password_secret_id: Optional[str] = None
    extra_settings: Dict[str, Any] = {}

    model_config = SettingsConfigDict(
        # HLD Mandate: Global Renaming (Ecosystem Name)
        env_prefix='OMOPCLOUDETL_CONN_',
        env_nested_delimiter='__',
        case_sensitive=False
    )
    # Note: Full password resolution validation will happen in the ConfigManager
    # after the secrets provider has been initialized.


class ProjectConfig(BaseModel):
    """The root configuration model for an omopcloudetl project."""
    connection: ConnectionConfig
    orchestrator: OrchestratorConfig
    schemas: Dict[str, str]
    secrets: Optional[SecretsConfig] = None

    @model_validator(mode='after')
    def check_secrets_configured_for_secret_id(self) -> 'ProjectConfig':
        """
        Validate that if a secret ID is used for the password and no direct
        password is provided, a secrets provider must also be configured.
        """
        if (
            self.connection.password_secret_id and
            not self.connection.password and
            not self.secrets
        ):
            raise ConfigurationError(
                "A secrets provider must be configured in 'secrets' when "
                "'connection.password_secret_id' is used without a direct 'password'."
            )
        return self
