"""Unit tests for the Redis client lifecycle (no Redis server required)."""

from collections.abc import AsyncIterator
from typing import cast

import fakeredis.aioredis
import pytest
import pytest_asyncio
from pydantic import SecretStr
from redis.asyncio import Redis
from redis.exceptions import ConnectionError as RedisConnectionError

from app.core.config import Settings
from app.infrastructure import redis as redis_infrastructure
from app.infrastructure.redis import (
    RedisClient,
    RedisUnavailableError,
    check_redis,
    create_redis_client,
    ensure_redis_ready,
)


class BrokenRedis:
    """Client whose ping always fails."""

    async def ping(self, *args: object, **kwargs: object) -> object:
        del args, kwargs
        raise RedisConnectionError("connection lost")


@pytest_asyncio.fixture
async def fake_client() -> AsyncIterator[fakeredis.aioredis.FakeRedis]:
    client = fakeredis.aioredis.FakeRedis(decode_responses=True)
    try:
        yield client
    finally:
        await client.aclose()


def test_settings_defaults_and_prefix_validation(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("REDIS_ENABLED", raising=False)
    monkeypatch.delenv("REDIS_KEY_PREFIX", raising=False)
    settings = Settings()

    assert settings.redis_enabled is True
    assert settings.redis_url.get_secret_value() == "redis://localhost:6379/0"
    assert settings.redis_key_prefix == "fleetpulse"
    assert settings.redis_connect_timeout_seconds > 0
    assert settings.redis_socket_timeout_seconds > 0
    assert settings.redis_max_connections >= 1
    assert settings.redis_location_stale_after_seconds >= 1

    normalised = Settings(redis_key_prefix="  :tenant-a:  ")
    assert normalised.redis_key_prefix == "tenant-a"

    with pytest.raises(ValueError):
        Settings(redis_key_prefix="bad prefix")


def test_redis_url_is_masked_in_settings_repr() -> None:
    settings = Settings(redis_url=SecretStr("redis://:super-secret@localhost:6379/0"))
    assert "super-secret" not in repr(settings)
    assert "super-secret" not in str(settings)


@pytest.mark.asyncio
async def test_create_redis_client_applies_connection_settings() -> None:
    settings = Settings(
        redis_connect_timeout_seconds=3.5,
        redis_socket_timeout_seconds=4.5,
        redis_max_connections=7,
    )
    client = create_redis_client(settings)
    try:
        kwargs = client.connection_pool.connection_kwargs
        assert kwargs["socket_connect_timeout"] == 3.5
        assert kwargs["socket_timeout"] == 4.5
        assert kwargs["decode_responses"] is True
        assert client.connection_pool.max_connections == 7
    finally:
        # No connection was opened, so this is immediate.
        await client.aclose()


@pytest.mark.asyncio
async def test_check_redis(fake_client: fakeredis.aioredis.FakeRedis) -> None:
    assert await check_redis(cast(Redis, fake_client)) is True
    assert await check_redis(cast(Redis, BrokenRedis())) is False


@pytest.mark.asyncio
async def test_ensure_redis_ready(fake_client: fakeredis.aioredis.FakeRedis) -> None:
    await ensure_redis_ready(cast(Redis, fake_client))

    with pytest.raises(RedisUnavailableError):
        await ensure_redis_ready(cast(Redis, BrokenRedis()))


@pytest.mark.asyncio
async def test_redis_client_lifecycle_uses_shared_client(
    monkeypatch: pytest.MonkeyPatch,
    fake_client: fakeredis.aioredis.FakeRedis,
) -> None:
    settings = Settings()
    monkeypatch.setattr(
        redis_infrastructure,
        "create_redis_client",
        lambda _settings: fake_client,
    )

    redis_client = RedisClient(settings)
    assert redis_client.client is fake_client

    await redis_client.start()
    await redis_client.close()
    # Closing is idempotent.
    await redis_client.close()

    with pytest.raises(RedisUnavailableError):
        await redis_client.start()
