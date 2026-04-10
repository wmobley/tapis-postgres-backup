from __future__ import annotations

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    TAPIS_BASE_URL: str = Field(default="https://portals.tapis.io")
    TAPIS_TENANT_ID: str = Field(default="portals")
    TAPIS_PODS_BASE_URL: str | None = Field(default=None)
    TAPIS_SERVICE_USERNAME: str | None = Field(default=None)
    TAPIS_SERVICE_PASSWORD: str | None = Field(default=None)
    TAS_USER: str | None = Field(default=None)
    TAS_SECRET: str | None = Field(default=None)

    TAPIS_BACKUP_SYSTEM_ID: str = Field(default="ptdatax.project.PTDATAX-284")
    TAPIS_BACKUP_ROOT_PATH: str = Field(default="/upstream-postgres")
    TAPIS_BACKUP_RETENTION_DAYS: int = Field(default=7)
    TAPIS_BACKUP_STAGING_DIR: str = Field(default="/tmp/upstream-postgres-backups")
    TAPIS_BACKUP_TIMEOUT_SECONDS: int = Field(default=300)

    DEFAULT_ADMIN_USERS: list[str] = Field(default_factory=lambda: ["wmobley", "tasclient_dsso"])

    TAPIS_POSTGRES_BACKUP_MODE: str = Field(default="backup-once")
    TAPIS_POSTGRES_BACKUP_INTERVAL_SECONDS: int = Field(default=86400)
    TAPIS_POSTGRES_BACKUP_RUN_IMMEDIATELY: bool = Field(default=True)
    TAPIS_POSTGRES_BACKUP_LOG_LEVEL: str = Field(default="INFO")

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )


def get_settings() -> Settings:
    return Settings()
