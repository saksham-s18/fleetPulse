"""Shipment domain model."""

from __future__ import annotations

import uuid
from datetime import datetime
from typing import TYPE_CHECKING, Any

from sqlalchemy import DateTime, Enum, String, func
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.infrastructure.database import Base
from app.models.enums import ShipmentStatus, enum_values

if TYPE_CHECKING:
    from app.models.vehicle_assignment import VehicleAssignment


class Shipment(Base):
    """A delivery request that can be assigned to a vehicle and driver."""

    __tablename__ = "shipments"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4,
    )
    reference: Mapped[str] = mapped_column(
        String(64),
        unique=True,
        nullable=False,
    )
    origin: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    destination: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    status: Mapped[ShipmentStatus] = mapped_column(
        Enum(
            ShipmentStatus,
            values_callable=enum_values,
            native_enum=False,
            create_constraint=True,
            validate_strings=True,
            length=32,
            name="shipment_status",
        ),
        nullable=False,
        default=ShipmentStatus.CREATED,
        server_default=ShipmentStatus.CREATED.value,
    )
    expected_delivery_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
        onupdate=func.now(),
    )

    assignments: Mapped[list[VehicleAssignment]] = relationship(
        back_populates="shipment",
        passive_deletes=True,
    )
