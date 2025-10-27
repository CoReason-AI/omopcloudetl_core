# Copyright (c) 2025 Scientific Informatics, LLC
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/omopcloudetl_core

from typing import Any, Dict, Optional

from pydantic import BaseModel, SecretStr, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


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
    password: Optional[SecretStr] = None
    password_secret_id: Optional[str] = None
    extra_settings: Dict[str, Any] = {}

    model_config = SettingsConfigDict(env_prefix="OMOPCLOUDETL_CONN_", env_nested_delimiter="__", case_sensitive=False)


class ProjectConfig(BaseModel):
    """The root configuration model for an omopcloudetl project."""

    connection: ConnectionConfig
    orchestrator: OrchestratorConfig
    schemas: Dict[str, str]
    secrets: Optional[SecretsConfig] = None

    @model_validator(mode="after")
    def check_secrets_provider_configured(self) -> "ProjectConfig":
        """Ensure that if a secret is used for the password, a secrets provider is configured."""
        if self.connection.password_secret_id and not self.connection.password:
            if not self.secrets:
                raise ValueError(
                    "A 'secrets' provider must be configured when 'connection.password_secret_id' is used."
                )
        return self
