# Copyright (c) 2025 Scientific Informatics, LLC
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/omopcloudetl_core

from pydantic import BaseModel, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict
from typing import Optional, Dict, Any

class SecretsConfig(BaseModel):
    provider_type: str
    configuration: Dict[str, Any] = {}

class OrchestratorConfig(BaseModel):
    type: str
    configuration: Dict[str, Any] = {}

class ConnectionConfig(BaseSettings):
    provider_type: str
    host: Optional[str] = None
    user: Optional[str] = None
    password: Optional[SecretStr] = None
    password_secret_id: Optional[str] = None
    extra_settings: Dict[str, Any] = {}

    model_config = SettingsConfigDict(
        env_prefix='OMOPCLOUDETL_CONN_',
        env_nested_delimiter='__',
        case_sensitive=False
    )

class ProjectConfig(BaseModel):
    connection: ConnectionConfig
    orchestrator: OrchestratorConfig
    schemas: Dict[str, str]
    secrets: Optional[SecretsConfig] = None
