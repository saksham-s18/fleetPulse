"""Application configuration loaded from environment variables."""

from functools import lru_cache
from typing import Literal

from pydantic import Field, SecretStr, field_validator
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

    # SecretStr keeps the DSN (and its password) out of reprs and log output.
    # The local default has no password; set DATABASE_URL in `.env` for Docker.
    database_url: SecretStr = SecretStr("postgresql+asyncpg://fleetpulse@localhost:5432/fleetpulse")
    db_pool_size: int = Field(default=5, ge=1, le=50)
    db_max_overflow: int = Field(default=10, ge=0, le=100)
    db_pool_timeout_seconds: float = Field(default=30.0, gt=0)
    db_connect_timeout_seconds: float = Field(default=5.0, gt=0)

    # Kafka is used for publishing validated GPS events and for the standalone
    # consumer worker. When enabled it is required: API startup fails if the
    # broker is unreachable. Set KAFKA_ENABLED=false to run the API without a
    # broker (unit tests); the consumer worker always requires it.
    kafka_enabled: bool = True
    kafka_bootstrap_servers: str = Field(default="localhost:9092", min_length=1)
    kafka_gps_topic: str = Field(default="gps.events", min_length=1, max_length=249)
    kafka_gps_partitions: int = Field(default=3, ge=1, le=1000)
    kafka_gps_replication_factor: int = Field(default=1, ge=1, le=1000)
    # Upper bound for waiting on broker acknowledgement. aiokafka retries
    # retriable errors indefinitely and has no delivery timeout, so the
    # publisher caps the total wait at this value (and uses it as the
    # per-request timeout); exceeding it fails the publication with 503.
    kafka_delivery_timeout_seconds: float = Field(default=30.0, gt=0)

    # Standalone GPS consumer worker (``python -m app.workers.gps_worker``).
    kafka_consumer_group_id: str = Field(
        default="fleetpulse-gps-processors", min_length=1, max_length=249
    )
    kafka_gps_dlq_topic: str = Field(default="gps.events.dlq", min_length=1, max_length=249)
    kafka_max_processing_attempts: int = Field(default=3, ge=1, le=10)
    kafka_retry_delay_seconds: float = Field(default=1.0, ge=0)
    kafka_consumer_poll_timeout_seconds: float = Field(default=1.0, gt=0)

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
