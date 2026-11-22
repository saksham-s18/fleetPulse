"""Vehicle assignment domain model."""

from __future__ import annotations

import uuid
from datetime import datetime
from typing import TYPE_CHECKING

from sqlalchemy import CheckConstraint, DateTime, ForeignKey, Index, func, text
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.infrastructure.database import Base

if TYPE_CHECKING:
    from app.models.driver import Driver
    from app.models.shipment import Shipment
    from app.models.vehicle import Vehicle


class VehicleAssignment(Base):
    """A historical link between a vehicle, a driver and a shipment.

    Active assignments are those with ``ended_at`` set to ``NULL``. PostgreSQL
    partial unique indexes enforce at most one active assignment per vehicle,
    driver and shipment, while ended rows remain available as history.
    """

    __tablename__ = "vehicle_assignments"
    __table_args__ = (
        CheckConstraint(
            "ended_at IS NULL OR ended_at >= assigned_at",
            name="assignment_time_range",
        ),
        Index("ix_vehicle_assignments_vehicle_id", "vehicle_id"),
        Index("ix_vehicle_assignments_driver_id", "driver_id"),
        Index("ix_vehicle_assignments_shipment_id", "shipment_id"),
        Index(
            "uq_vehicle_assignments_active_vehicle",
            "vehicle_id",
            unique=True,
            postgresql_where=text("ended_at IS NULL"),
        ),
        Index(
            "uq_vehicle_assignments_active_driver",
            "driver_id",
            unique=True,
            postgresql_where=text("ended_at IS NULL"),
        ),
        Index(
            "uq_vehicle_assignments_active_shipment",
            "shipment_id",
            unique=True,
            postgresql_where=text("ended_at IS NULL"),
        ),
    )

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4,
    )
    vehicle_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("vehicles.id", ondelete="RESTRICT"),
        nullable=False,
    )
    driver_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("drivers.id", ondelete="RESTRICT"),
        nullable=False,
    )
    shipment_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("shipments.id", ondelete="RESTRICT"),
        nullable=False,
    )
    assigned_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )
    ended_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )

    vehicle: Mapped[Vehicle] = relationship(back_populates="assignments")
    driver: Mapped[Driver] = relationship(back_populates="assignments")
    shipment: Mapped[Shipment] = relationship(back_populates="assignments")
