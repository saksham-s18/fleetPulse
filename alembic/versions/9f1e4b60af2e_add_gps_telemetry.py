"""add gps telemetry

Revision ID: 9f1e4b60af2e
Revises: 05a337c43e7f
Create Date: 2026-11-27 18:38:21.963180

"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "9f1e4b60af2e"
down_revision: str | Sequence[str] | None = "05a337c43e7f"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "gps_telemetry",
        sa.Column("event_id", sa.UUID(), nullable=False),
        sa.Column("vehicle_id", sa.UUID(), nullable=False),
        sa.Column("shipment_id", sa.UUID(), nullable=True),
        sa.Column("latitude", sa.Float(), nullable=False),
        sa.Column("longitude", sa.Float(), nullable=False),
        sa.Column("speed_kmh", sa.Float(), nullable=False),
        sa.Column("heading", sa.Float(), nullable=True),
        sa.Column("recorded_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column(
            "received_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("schema_version", sa.String(length=32), server_default="1.0", nullable=False),
        sa.CheckConstraint(
            "heading IS NULL OR (heading >= 0 AND heading <= 360)",
            name=op.f("ck_gps_telemetry_heading_range"),
        ),
        sa.CheckConstraint(
            "latitude >= -90 AND latitude <= 90",
            name=op.f("ck_gps_telemetry_latitude_range"),
        ),
        sa.CheckConstraint(
            "longitude >= -180 AND longitude <= 180",
            name=op.f("ck_gps_telemetry_longitude_range"),
        ),
        sa.CheckConstraint(
            "speed_kmh >= 0",
            name=op.f("ck_gps_telemetry_speed_kmh_non_negative"),
        ),
        sa.ForeignKeyConstraint(
            ["shipment_id"],
            ["shipments.id"],
            name=op.f("fk_gps_telemetry_shipment_id_shipments"),
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["vehicle_id"],
            ["vehicles.id"],
            name=op.f("fk_gps_telemetry_vehicle_id_vehicles"),
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint("event_id", name=op.f("pk_gps_telemetry")),
    )
    op.create_index(
        "ix_gps_telemetry_shipment_recorded_at",
        "gps_telemetry",
        ["shipment_id", sa.text("recorded_at DESC")],
        unique=False,
        postgresql_where=sa.text("shipment_id IS NOT NULL"),
    )
    op.create_index(
        "ix_gps_telemetry_vehicle_recorded_at",
        "gps_telemetry",
        ["vehicle_id", sa.text("recorded_at DESC")],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index(
        "ix_gps_telemetry_shipment_recorded_at",
        table_name="gps_telemetry",
        postgresql_where=sa.text("shipment_id IS NOT NULL"),
    )
    op.drop_index("ix_gps_telemetry_vehicle_recorded_at", table_name="gps_telemetry")
    op.drop_table("gps_telemetry")
