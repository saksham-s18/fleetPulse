"""Public error response schemas."""

from typing import Any

from pydantic import BaseModel


class ErrorDetail(BaseModel):
    """Machine-readable error code plus a safe human-readable message."""

    code: str
    message: str
    details: Any = None


class ErrorResponse(BaseModel):
    """Consistent envelope used for every error response."""

    error: ErrorDetail
