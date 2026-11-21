"""Unit tests for engine configuration and session lifecycle handling."""

import logging
from typing import cast

import pytest
from pydantic import SecretStr
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from sqlalchemy.pool import QueuePool

from app.core.config import Settings
from app.infrastructure.database import check_database, create_engine, get_db_session


class _FakeSession:
    """Minimal async context manager standing in for an AsyncSession."""

    def __init__(self) -> None:
        self.closed = False
        self.rolled_back = False

    async def __aenter__(self) -> "_FakeSession":
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: object,
    ) -> None:
        self.closed = True

    async def rollback(self) -> None:
        self.rolled_back = True


class _FakeSessionFactory:
    """Callable factory returning a single fake session."""

    def __init__(self, session: _FakeSession) -> None:
        self._session = session

    def __call__(self) -> _FakeSession:
        return self._session


@pytest.mark.asyncio
async def test_create_engine_uses_asyncpg_and_configured_pool_size() -> None:
    settings = Settings(
        database_url=SecretStr("postgresql+asyncpg://fleetpulse@localhost:5432/fleetpulse"),
        db_pool_size=3,
    )

    engine = create_engine(settings)

    try:
        assert engine.url.drivername == "postgresql+asyncpg"
        assert isinstance(engine.pool, QueuePool)
        assert engine.pool.size() == 3
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_session_dependency_yields_and_closes_session() -> None:
    session = _FakeSession()
    factory = cast(async_sessionmaker[AsyncSession], _FakeSessionFactory(session))
    dependency = get_db_session(factory)

    yielded = await anext(dependency)

    assert isinstance(yielded, _FakeSession)
    assert yielded is session
    assert session.closed is False

    await dependency.aclose()

    assert session.closed is True


@pytest.mark.asyncio
async def test_session_dependency_rolls_back_and_closes_on_error() -> None:
    session = _FakeSession()
    factory = cast(async_sessionmaker[AsyncSession], _FakeSessionFactory(session))
    dependency = get_db_session(factory)
    await anext(dependency)

    with pytest.raises(RuntimeError, match="boom"):
        await dependency.athrow(RuntimeError("boom"))

    assert session.rolled_back is True
    assert session.closed is True


def test_database_url_is_masked_in_settings_repr() -> None:
    settings = Settings(
        database_url=SecretStr(
            "postgresql+asyncpg://fleetpulse:supersecret@localhost:5432/fleetpulse"
        )
    )

    assert "supersecret" not in repr(settings)


@pytest.mark.asyncio
async def test_check_database_failure_does_not_log_credentials(
    caplog: pytest.LogCaptureFixture,
) -> None:
    settings = Settings(
        database_url=SecretStr(
            "postgresql+asyncpg://fleetpulse:supersecret@127.0.0.1:1/fleetpulse"
        ),
        db_connect_timeout_seconds=1.0,
    )
    engine = create_engine(settings)

    try:
        with caplog.at_level(logging.WARNING, logger="app.infrastructure.database"):
            available = await check_database(engine)
    finally:
        await engine.dispose()

    assert available is False
    assert "Database readiness check failed" in caplog.text
    assert "supersecret" not in caplog.text
