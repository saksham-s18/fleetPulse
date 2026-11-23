"""Domain exceptions mapped to consistent HTTP error responses.

Services raise these exceptions instead of FastAPI's ``HTTPException`` so the
business layer stays independent of the web framework. The API error handlers
translate them into the public error envelope.
"""

from typing import Any


class FleetPulseError(Exception):
    """Base class for expected application errors."""

    status_code: int = 500
    code: str = "internal_error"

    def __init__(self, message: str, *, details: Any = None) -> None:
        super().__init__(message)
        self.message = message
        self.details = details


class BadRequestError(FleetPulseError):
    """The request is syntactically valid but semantically invalid."""

    status_code = 400
    code = "bad_request"


class NotFoundError(FleetPulseError):
    """A requested entity does not exist."""

    status_code = 404
    code = "not_found"


class ConflictError(FleetPulseError):
    """The request conflicts with current state or a uniqueness constraint."""

    status_code = 409
    code = "conflict"


class ServiceUnavailableError(FleetPulseError):
    """A downstream delivery dependency is temporarily unavailable.

    Used when a validated GPS event cannot be handed to the ingestion
    pipeline (Kafka). The response never contains broker details.
    """

    status_code = 503
    code = "service_unavailable"
