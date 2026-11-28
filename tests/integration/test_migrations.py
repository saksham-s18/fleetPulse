"""Integration test for the GPS telemetry Alembic migration.

The test uses a temporary PostgreSQL database so the configured development
database is never upgraded or downgraded by the test suite.
"""

import asyncio
import os
from collections.abc import Mapping
from pathlib import Path
from uuid import uuid4

import asyncpg
import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy.engine import URL, make_url

from app.core.config import get_settings

pytestmark = pytest.mark.integration

PROJECT_ROOT = Path(__file__).resolve().parents[2]
BASELINE_REVISION = "05a337c43e7f"
TELEMETRY_REVISION = "9f1e4b60af2e"


def admin_url(database_url: URL) -> URL:
    """Return a synchronous PostgreSQL URL for the maintenance database."""
    return database_url.set(drivername="postgresql", database="postgres")


def dsn(url: URL) -> str:
    """Render a SQLAlchemy URL as an asyncpg-compatible DSN."""
    return url.set(drivername="postgresql").render_as_string(hide_password=False)


async def create_database(database_dsn: str, name: str) -> None:
    connection = await asyncpg.connect(database_dsn)
    try:
        await connection.execute(f'CREATE DATABASE "{name}"')
    finally:
        await connection.close()


async def drop_database(database_dsn: str, name: str) -> None:
    connection = await asyncpg.connect(database_dsn)
    try:
        await connection.execute(f'DROP DATABASE IF EXISTS "{name}"')
    finally:
        await connection.close()


async def table_names(database_dsn: str) -> set[str]:
    connection = await asyncpg.connect(database_dsn)
    try:
        rows = await connection.fetch(
            """
            SELECT tablename
            FROM pg_tables
            WHERE schemaname = 'public'
            """
        )
    finally:
        await connection.close()

    return {str(row["tablename"]) for row in rows}


async def column_map(database_dsn: str) -> dict[str, list[str]]:
    connection = await asyncpg.connect(database_dsn)
    try:
        rows = await connection.fetch(
            """
            SELECT table_name, column_name
            FROM information_schema.columns
            WHERE table_schema = 'public'
            ORDER BY table_name, ordinal_position
            """
        )
    finally:
        await connection.close()

    columns: dict[str, list[str]] = {}
    for row in rows:
        columns.setdefault(str(row["table_name"]), []).append(str(row["column_name"]))
    return columns


def existing_table_columns(columns: Mapping[str, list[str]]) -> dict[str, list[str]]:
    """Return the domain tables that existed before the telemetry migration."""
    return {
        table_name: table_columns
        for table_name, table_columns in columns.items()
        if table_name not in {"gps_telemetry"}
    }


def test_gps_telemetry_migration_upgrade_downgrade_reupgrade() -> None:
    configured_url = make_url(get_settings().database_url.get_secret_value())
    maintenance_dsn = dsn(admin_url(configured_url))
    temporary_name = f"fleetpulse_migration_{uuid4().hex[:12]}"
    temporary_url = configured_url.set(database=temporary_name)
    temporary_dsn = dsn(temporary_url)
    temporary_database_url = temporary_url.render_as_string(hide_password=False)

    previous_database_url = os.environ.get("DATABASE_URL")
    alembic_config = Config(str(PROJECT_ROOT / "alembic.ini"))
    alembic_config.set_main_option("script_location", str(PROJECT_ROOT / "alembic"))

    asyncio.run(create_database(maintenance_dsn, temporary_name))
    os.environ["DATABASE_URL"] = temporary_database_url
    get_settings.cache_clear()

    try:
        command.upgrade(alembic_config, BASELINE_REVISION)
        baseline_tables = asyncio.run(table_names(temporary_dsn))
        baseline_columns = existing_table_columns(asyncio.run(column_map(temporary_dsn)))
        assert "gps_telemetry" not in baseline_tables
        assert {"vehicles", "shipments", "vehicle_assignments", "drivers"} <= baseline_tables

        command.upgrade(alembic_config, TELEMETRY_REVISION)
        upgraded_tables = asyncio.run(table_names(temporary_dsn))
        upgraded_columns = existing_table_columns(asyncio.run(column_map(temporary_dsn)))
        assert upgraded_tables == baseline_tables | {"gps_telemetry"}
        assert upgraded_columns == baseline_columns

        command.downgrade(alembic_config, BASELINE_REVISION)
        downgraded_tables = asyncio.run(table_names(temporary_dsn))
        downgraded_columns = existing_table_columns(asyncio.run(column_map(temporary_dsn)))
        assert downgraded_tables == baseline_tables
        assert downgraded_columns == baseline_columns

        command.upgrade(alembic_config, TELEMETRY_REVISION)
        reupgraded_tables = asyncio.run(table_names(temporary_dsn))
        reupgraded_columns = existing_table_columns(asyncio.run(column_map(temporary_dsn)))
        assert reupgraded_tables == baseline_tables | {"gps_telemetry"}
        assert reupgraded_columns == baseline_columns
    finally:
        if previous_database_url is None:
            os.environ.pop("DATABASE_URL", None)
        else:
            os.environ["DATABASE_URL"] = previous_database_url
        get_settings.cache_clear()
        asyncio.run(drop_database(maintenance_dsn, temporary_name))
