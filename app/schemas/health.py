"""Pydantic response models for health probes."""

from typing import Literal

from pydantic import BaseModel, Field


class LivenessResponse(BaseModel):
    """Response returned when the process is alive."""

    status: Literal["ok"] = "ok"


class ReadinessResponse(BaseModel):
    """Response returned when the service is ready to accept traffic."""

    status: Literal["ready"] = "ready"
    checks: dict[str, str] = Field(default_factory=dict)
