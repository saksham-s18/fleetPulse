"""Pydantic response models for health probes."""

from typing import Literal

from pydantic import BaseModel, Field


class LivenessResponse(BaseModel):
    """Response returned when the process is alive."""

    status: Literal["ok"] = "ok"


class ReadinessResponse(BaseModel):
    """Response describing whether the service can accept traffic."""

    status: Literal["ready", "not_ready"]
    checks: dict[str, str] = Field(default_factory=dict)
