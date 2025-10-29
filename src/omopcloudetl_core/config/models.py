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
from typing import Any, Dict, Optional, Self

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
    def _validate_passwords(self) -> Self:
        if self.user and not self.password and not self.password_secret_id:
            raise ValueError("A password or password_secret_id is required when a user is provided.")
        if self.password and self.password_secret_id:
            warnings.warn(
                "Both 'password' and 'password_secret_id' are provided. The direct 'password' will be used.",
                UserWarning,
            )
        return self


class ProjectConfig(BaseModel):
    """The root configuration model for an omopcloudetl project."""

    connection: ConnectionConfig
    orchestrator: OrchestratorConfig
    schemas: Dict[str, str]
    secrets: Optional[SecretsConfig] = None
