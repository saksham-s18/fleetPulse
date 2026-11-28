"""Durable GPS telemetry model.

PostgreSQL is the source of truth for historical GPS observations; Redis
remains the rebuildable live-location projection. ``event_id`` is the primary
key so the database itself provides durable per-event idempotency.
"""

from __future__ import annotations

import uuid
from datetime import datetime

from sqlalchemy import (
    CheckConstraint,
    DateTime,
    Float,
    ForeignKey,
    Index,
    String,
    desc,
    func,
    text,
)
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column

from app.infrastructure.database import Base
from app.schemas.gps import GPS_SCHEMA_VERSION


class GpsTelemetry(Base):
    """One durable GPS observation reported by a vehicle."""

    __tablename__ = "gps_telemetry"
    __table_args__ = (
        CheckConstraint(
            "latitude >= -90 AND latitude <= 90",
            name="latitude_range",
        ),
        CheckConstraint(
            "longitude >= -180 AND longitude <= 180",
            name="longitude_range",
        ),
        CheckConstraint(
            "speed_kmh >= 0",
            name="speed_kmh_non_negative",
        ),
        CheckConstraint(
            "heading IS NULL OR (heading >= 0 AND heading <= 360)",
            name="heading_range",
        ),
        # Newest-first vehicle history. PostgreSQL can also scan this index
        # backwards, but the explicit DESC ordering documents the access path.
        Index(
            "ix_gps_telemetry_vehicle_recorded_at",
            "vehicle_id",
            desc("recorded_at"),
        ),
        # Shipment history only needs rows that actually reference a shipment;
        # the partial predicate keeps the index small for unassigned events.
        Index(
            "ix_gps_telemetry_shipment_recorded_at",
            "shipment_id",
            desc("recorded_at"),
            postgresql_where=text("shipment_id IS NOT NULL"),
        ),
    )

    event_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        primary_key=True,
    )
    vehicle_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("vehicles.id", ondelete="RESTRICT"),
        nullable=False,
    )
    shipment_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("shipments.id", ondelete="RESTRICT"),
        nullable=True,
    )
    latitude: Mapped[float] = mapped_column(Float, nullable=False)
    longitude: Mapped[float] = mapped_column(Float, nullable=False)
    speed_kmh: Mapped[float] = mapped_column(Float, nullable=False)
    heading: Mapped[float | None] = mapped_column(Float, nullable=True)
    recorded_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
    )
    received_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )
    schema_version: Mapped[str] = mapped_column(
        String(32),
        nullable=False,
        default=GPS_SCHEMA_VERSION,
        server_default=GPS_SCHEMA_VERSION,
    )
