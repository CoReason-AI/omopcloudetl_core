# Copyright (c) 2025 Scientific Informatics, LLC
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/omopcloudetl_core

import warnings
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

    model_config = SettingsConfigDict(
        env_prefix="OMOPCLOUDETL_CONN_",
        env_nested_delimiter="__",
        case_sensitive=False,
    )

    @model_validator(mode="after")
    def check_password_logic(self) -> "ConnectionConfig":
        if self.user and not (self.password or self.password_secret_id):
            raise ValueError(
                f"A 'user' is specified for provider '{self.provider_type}', "
                "but no 'password' or 'password_secret_id' was provided."
            )
        if self.password and self.password_secret_id:
            warnings.warn(
                f"Both 'password' and 'password_secret_id' are provided "
                f"for provider '{self.provider_type}'. "
                "The direct 'password' will be used, and the secret will not be resolved."
            )
        return self


class ProjectConfig(BaseModel):
    """Root configuration model for the project."""

    connection: ConnectionConfig
    orchestrator: OrchestratorConfig
    schemas: Dict[str, str]
    secrets: Optional[SecretsConfig] = None
