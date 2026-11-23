"""Publish GPS events without coupling the simulator to a specific transport."""

import asyncio
import json
import logging
from typing import Protocol
from urllib.error import HTTPError
from urllib.parse import urlparse
from urllib.request import Request, urlopen

from app.schemas.gps import GpsEvent

logger = logging.getLogger(__name__)


class TransportError(RuntimeError):
    """Raised when an event could not be delivered to the ingestion endpoint."""


class EventPublisher(Protocol):
    """Minimal async publisher contract shared by all transports."""

    async def publish(self, event: GpsEvent) -> None:
        """Deliver one event or raise :class:`TransportError`."""
        ...

    async def close(self) -> None:
        """Release transport resources."""
        ...


def _api_error_suffix(exc: HTTPError) -> str:
    """Extract the API's safe ``error.message`` from an HTTP error body.

    The FleetPulse API answers with a structured error envelope; surfacing its
    message makes failed publications diagnosable. Any unreadable or
    unexpected body is ignored rather than failing the transport itself.
    """
    try:
        raw = exc.read()
    except (AttributeError, OSError, ValueError):
        return ""
    if not raw:
        return ""
    try:
        document = json.loads(raw)
    except (json.JSONDecodeError, UnicodeDecodeError):
        return ""
    if not isinstance(document, dict):
        return ""
    error = document.get("error")
    if not isinstance(error, dict):
        return ""
    message = error.get("message")
    if isinstance(message, str) and message:
        return f": {message}"
    return ""


class HttpEventPublisher:
    """POST newline-free JSON documents to an HTTP ingestion endpoint.

    The standard library is used deliberately: the simulator has no runtime
    dependency beyond the application's existing requirements, and the
    transport can be replaced later (for example by a Kafka publisher) without
    touching event generation or the simulation loop.
    """

    def __init__(self, endpoint: str, *, timeout_seconds: float = 10.0) -> None:
        parsed = urlparse(endpoint)
        if parsed.scheme not in {"http", "https"} or not parsed.netloc:
            raise ValueError("ingestion endpoint must be an absolute http(s) URL")
        if timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be positive")

        self._endpoint = endpoint
        self._timeout_seconds = timeout_seconds

    async def publish(self, event: GpsEvent) -> None:
        body = json.dumps(event.model_dump(mode="json"), separators=(",", ":")).encode("utf-8")
        await asyncio.to_thread(self._post, body)

    def _post(self, body: bytes) -> None:
        request = Request(
            self._endpoint,
            data=body,
            headers={
                "Accept": "application/json",
                "Content-Type": "application/json",
            },
            method="POST",
        )

        try:
            with urlopen(request, timeout=self._timeout_seconds) as response:
                status = response.status
        except HTTPError as exc:
            raise TransportError(
                f"ingestion endpoint returned HTTP {exc.code} for {self._endpoint}"
                f"{_api_error_suffix(exc)}"
            ) from exc
        except OSError as exc:
            raise TransportError(
                f"could not reach ingestion endpoint {self._endpoint}: {exc}"
            ) from exc

        if not 200 <= status < 300:
            raise TransportError(f"ingestion endpoint returned HTTP {status} for {self._endpoint}")

        logger.debug("Ingestion endpoint accepted event (HTTP %d)", status)

    async def close(self) -> None:
        """No persistent resources are held by the HTTP transport."""


class DryRunPublisher:
    """Log events instead of sending them."""

    async def publish(self, event: GpsEvent) -> None:
        payload = json.dumps(event.model_dump(mode="json"), separators=(",", ":"))
        logger.info("gps_event %s", payload)

    async def close(self) -> None:
        """No persistent resources are held by the dry-run transport."""
