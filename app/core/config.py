"""Application configuration loaded from environment variables."""

from functools import lru_cache
from typing import Literal

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

AppEnvironment = Literal["development", "staging", "production", "test"]
LogLevel = Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]


class Settings(BaseSettings):
    """Runtime settings for FleetPulse.

    Values are read from environment variables (for example ``APP_NAME``) or
    from a local ``.env`` file. Environment variables take precedence.
    """

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    app_name: str = Field(default="FleetPulse API", min_length=1)
    app_version: str = Field(default="0.1.0", min_length=1)
    app_env: AppEnvironment = "development"
    log_level: LogLevel = "INFO"

    @field_validator("app_env", mode="before")
    @classmethod
    def _normalise_environment(cls, value: object) -> object:
        """Accept environment names in any casing, e.g. ``Development``."""
        return value.strip().lower() if isinstance(value, str) else value

    @field_validator("log_level", mode="before")
    @classmethod
    def _normalise_log_level(cls, value: object) -> object:
        """Accept log levels in any casing, e.g. ``info``."""
        return value.strip().upper() if isinstance(value, str) else value

    @property
    def is_development(self) -> bool:
        """Return ``True`` when interactive API documentation should be enabled."""
        return self.app_env == "development"


@lru_cache
def get_settings() -> Settings:
    """Return the process-wide settings instance, creating it on first use."""
    return Settings()
