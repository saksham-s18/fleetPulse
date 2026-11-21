"""Async SQLAlchemy engine, session factory and declarative base."""

import logging
from collections.abc import AsyncGenerator
from typing import Annotated

from fastapi import Depends, Request
from sqlalchemy import MetaData, text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.orm import DeclarativeBase

from app.core.config import Settings

logger = logging.getLogger(__name__)

# Stable constraint names keep Alembic migrations predictable.
NAMING_CONVENTION = {
    "ix": "ix_%(column_0_label)s",
    "uq": "uq_%(table_name)s_%(column_0_name)s",
    "ck": "ck_%(table_name)s_%(constraint_name)s",
    "fk": "fk_%(table_name)s_%(column_0_name)s_%(referred_table_name)s",
    "pk": "pk_%(table_name)s",
}

POOL_RECYCLE_SECONDS = 1800


class Base(DeclarativeBase):
    """Declarative base for SQLAlchemy 2.x typed ORM models."""

    metadata = MetaData(naming_convention=NAMING_CONVENTION)


def create_engine(settings: Settings) -> AsyncEngine:
    """Create the application's async engine.

    Creating an engine does not open a database connection. The engine should
    be created once per application process and disposed during shutdown.
    """
    return create_async_engine(
        settings.database_url.get_secret_value(),
        pool_size=settings.db_pool_size,
        max_overflow=settings.db_max_overflow,
        pool_timeout=settings.db_pool_timeout_seconds,
        pool_recycle=POOL_RECYCLE_SECONDS,
        pool_pre_ping=True,
        connect_args={"timeout": settings.db_connect_timeout_seconds},
    )


def create_session_factory(engine: AsyncEngine) -> async_sessionmaker[AsyncSession]:
    """Create an async session factory bound to the given engine."""
    return async_sessionmaker(
        bind=engine,
        expire_on_commit=False,
        autoflush=False,
    )


def get_database_engine(request: Request) -> AsyncEngine:
    """FastAPI dependency returning the application's shared engine."""
    engine: AsyncEngine = request.app.state.db_engine
    return engine


def get_session_factory(request: Request) -> async_sessionmaker[AsyncSession]:
    """FastAPI dependency returning the application's session factory."""
    session_factory: async_sessionmaker[AsyncSession] = request.app.state.db_session_factory
    return session_factory


async def get_db_session(
    session_factory: Annotated[async_sessionmaker[AsyncSession], Depends(get_session_factory)],
) -> AsyncGenerator[AsyncSession, None]:
    """Yield a request-scoped session and always close it.

    Transactions are not committed automatically; callers decide when to
    commit. Any exception rolls the session back before it is closed.
    """
    async with session_factory() as session:
        try:
            yield session
        except Exception:
            await session.rollback()
            raise


async def check_database(engine: AsyncEngine) -> bool:
    """Return ``True`` when PostgreSQL answers a simple query.

    Failures are logged by exception type only, so connection strings and
    driver details never leak into logs or health responses.
    """
    try:
        async with engine.connect() as connection:
            await connection.execute(text("SELECT 1"))
    except (SQLAlchemyError, OSError, TimeoutError) as exc:
        logger.warning("Database readiness check failed (%s)", type(exc).__name__)
        return False
    return True
