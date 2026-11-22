"""Driver domain model."""

from __future__ import annotations

import uuid
from datetime import datetime
from typing import TYPE_CHECKING

from sqlalchemy import DateTime, Enum, String, func
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.infrastructure.database import Base
from app.models.enums import DriverStatus, enum_values

if TYPE_CHECKING:
    from app.models.vehicle_assignment import VehicleAssignment


class Driver(Base):
    """A driver who can be assigned to vehicles and shipments."""

    __tablename__ = "drivers"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4,
    )
    full_name: Mapped[str] = mapped_column(String(200), nullable=False)
    phone: Mapped[str | None] = mapped_column(
        String(32),
        unique=True,
        nullable=True,
    )
    license_number: Mapped[str] = mapped_column(
        String(64),
        unique=True,
        nullable=False,
    )
    status: Mapped[DriverStatus] = mapped_column(
        Enum(
            DriverStatus,
            values_callable=enum_values,
            native_enum=False,
            create_constraint=True,
            validate_strings=True,
            length=32,
            name="driver_status",
        ),
        nullable=False,
        default=DriverStatus.ACTIVE,
        server_default=DriverStatus.ACTIVE.value,
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
        back_populates="driver",
        passive_deletes=True,
    )
