from pydantic import BaseModel, SecretStr, Field
from pydantic_settings import BaseSettings, SettingsConfigDict
from typing import Optional, Dict, Any

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
