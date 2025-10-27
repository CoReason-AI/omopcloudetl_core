# Copyright (c) 2025 Scientific Informatics, LLC
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/omopcloudetl_core


from pydantic import BaseModel, Field, SecretStr, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict
from typing import Any, Dict, Optional


class SecretsConfig(BaseModel):
    provider_type: str
    configuration: Dict[str, Any] = Field(default_factory=dict)


class OrchestratorConfig(BaseModel):
    type: str
    configuration: Dict[str, Any] = Field(default_factory=dict)


class ConnectionConfig(BaseSettings):
    provider_type: str
    host: Optional[str] = None
    user: Optional[str] = None
    password: Optional[SecretStr] = None
    password_secret_id: Optional[str] = None
    extra_settings: Dict[str, Any] = Field(default_factory=dict)

    model_config = SettingsConfigDict(env_prefix="OMOPCLOUDETL_CONN_", env_nested_delimiter="__", case_sensitive=False)


class ProjectConfig(BaseModel):
    connection: ConnectionConfig
    orchestrator: OrchestratorConfig
    schemas: Dict[str, str]
    secrets: Optional[SecretsConfig] = None

    @model_validator(mode="after")
    def check_secret_id_requires_secrets_config(self) -> "ProjectConfig":
        """
        Validate that if a secret ID is used for the password, a secrets provider
        is also configured.
        """
        if self.connection.password_secret_id and not self.connection.password and not self.secrets:
            raise ValueError(
                "A 'password_secret_id' was provided for the connection without a password, "
                "but no 'secrets' provider configuration was found in the project file."
            )
        return self
