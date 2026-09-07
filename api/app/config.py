"""Application configuration loaded from environment variables / .env file."""
from functools import lru_cache
from typing import List, Literal

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    # --- ControlDB connection ---
    sql_server: str = Field(..., description="ControlDB server hostname, e.g. myserver.database.windows.net")
    sql_database: str = Field(default="controlDB")
    sql_auth_mode: Literal["ServicePrincipal", "ManagedIdentity"] = Field(default="ServicePrincipal")

    aad_tenant_id: str | None = None
    aad_client_id: str | None = None
    aad_client_secret: str | None = None

    sql_pool_min_size: int = 1
    sql_pool_max_size: int = 10
    sql_command_timeout_seconds: int = 30

    # --- API authentication (validating caller tokens) ---
    api_aad_tenant_id: str = Field(..., description="Entra tenant ID that issues caller tokens")
    api_aad_audience: str = Field(..., description="Expected audience / App ID URI for this API")
    api_aad_allowed_app_ids: str = Field(default="", description="Comma-separated allowed caller appid/azp claims")
    auth_disabled: bool = Field(default=False, description="Disable auth validation (local dev only)")

    log_level: str = Field(default="INFO")

    @field_validator("api_aad_allowed_app_ids")
    @classmethod
    def _normalize_app_ids(cls, v: str) -> str:
        return v.strip()

    @property
    def allowed_app_ids(self) -> List[str]:
        return [a.strip() for a in self.api_aad_allowed_app_ids.split(",") if a.strip()]

    @property
    def aad_authority(self) -> str:
        return f"https://login.microsoftonline.com/{self.api_aad_tenant_id}/v2.0"

    @property
    def aad_jwks_uri(self) -> str:
        return f"https://login.microsoftonline.com/{self.api_aad_tenant_id}/discovery/v2.0/keys"


@lru_cache
def get_settings() -> Settings:
    return Settings()  # type: ignore[call-arg]
