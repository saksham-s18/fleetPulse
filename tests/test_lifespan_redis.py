"""Unit tests for the Redis client lifecycle managed by the FastAPI lifespan."""

from collections.abc import Iterator
from typing import Any, ClassVar

import pytest
from fastapi.testclient import TestClient

from app.core.config import get_settings
from app.infrastructure.redis import RedisUnavailableError
from app.main import create_app


class FakeRedisClient:
    """Stand-in for RedisClient recording lifecycle calls."""

    instances: ClassVar[list["FakeRedisClient"]] = []
    start_error: ClassVar[BaseException | None] = None

    def __init__(self, settings: Any) -> None:
        self.settings = settings
        self.client = object()
        self.started = False
        self.closed = False
        FakeRedisClient.instances.append(self)

    async def start(self) -> None:
        if FakeRedisClient.start_error is not None:
            raise FakeRedisClient.start_error
        self.started = True

    async def close(self) -> None:
        self.closed = True


@pytest.fixture(autouse=True)
def fake_redis_client(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    FakeRedisClient.instances.clear()
    FakeRedisClient.start_error = None
    monkeypatch.setattr("app.main.RedisClient", FakeRedisClient)
    yield


def test_redis_disabled_skips_readiness_check_but_closes_client() -> None:
    settings = get_settings()
    assert settings.redis_enabled is False, "the unit suite must not require Redis"

    app = create_app(settings)
    with TestClient(app):
        assert app.state.redis_client is FakeRedisClient.instances[0].client

    assert len(FakeRedisClient.instances) == 1
    assert FakeRedisClient.instances[0].started is False
    assert FakeRedisClient.instances[0].closed is True


def test_redis_enabled_pings_on_startup_and_closes_on_shutdown() -> None:
    settings = get_settings().model_copy(update={"redis_enabled": True})

    app = create_app(settings)
    with TestClient(app):
        assert app.state.redis_client is FakeRedisClient.instances[0].client
        assert FakeRedisClient.instances[0].started is True
        assert FakeRedisClient.instances[0].closed is False

    assert len(FakeRedisClient.instances) == 1
    assert FakeRedisClient.instances[0].closed is True


def test_startup_fails_loudly_when_required_redis_cannot_start() -> None:
    settings = get_settings().model_copy(update={"redis_enabled": True})
    FakeRedisClient.start_error = RedisUnavailableError("redis unreachable")

    with (
        pytest.raises(RuntimeError, match="Redis is required"),
        TestClient(create_app(settings)),
    ):
        pass  # pragma: no cover - startup must fail before this point

    assert FakeRedisClient.instances[0].closed is True
