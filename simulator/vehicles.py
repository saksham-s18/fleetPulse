"""Discover active FleetPulse vehicles that the simulator can drive."""

import asyncio
import json
from urllib.parse import urlencode
from urllib.request import urlopen
from uuid import UUID

from app.models.enums import VehicleStatus

DEFAULT_PAGE_SIZE = 100


class VehicleDiscoveryError(RuntimeError):
    """Raised when active vehicles cannot be loaded from the FleetPulse API."""


def _vehicles_url(api_url: str, page: int, page_size: int) -> str:
    query = urlencode(
        {
            "status": VehicleStatus.ACTIVE.value,
            "page": page,
            "page_size": page_size,
        }
    )
    return f"{api_url.rstrip('/')}/api/v1/vehicles?{query}"


def _get_json(url: str, timeout_seconds: float) -> object:
    try:
        with urlopen(url, timeout=timeout_seconds) as response:
            charset = response.headers.get_content_charset() or "utf-8"
            body = response.read()
    except OSError as exc:
        # URLError and HTTPError are both OSError subclasses.
        raise VehicleDiscoveryError(f"could not load vehicles from {url}: {exc}") from exc

    try:
        return json.loads(body.decode(charset))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise VehicleDiscoveryError("vehicle discovery returned invalid JSON") from exc


async def fetch_active_vehicle_ids(
    api_url: str,
    *,
    page_size: int = DEFAULT_PAGE_SIZE,
    timeout_seconds: float = 10.0,
) -> list[UUID]:
    """Return every active vehicle ID, following API pagination."""
    if page_size < 1:
        raise ValueError("page_size must be at least 1")
    if timeout_seconds <= 0:
        raise ValueError("timeout_seconds must be positive")

    vehicle_ids: list[UUID] = []
    page = 1

    while True:
        url = _vehicles_url(api_url, page, page_size)
        payload = await asyncio.to_thread(_get_json, url, timeout_seconds)

        if not isinstance(payload, dict):
            raise VehicleDiscoveryError("vehicle discovery returned an unexpected payload")

        items = payload.get("items")
        if not isinstance(items, list):
            raise VehicleDiscoveryError("vehicle discovery response has no items list")

        for item in items:
            if not isinstance(item, dict):
                raise VehicleDiscoveryError("vehicle discovery returned an invalid item")
            if item.get("status") != VehicleStatus.ACTIVE.value:
                continue

            raw_id = item.get("id")
            if not isinstance(raw_id, str):
                raise VehicleDiscoveryError("vehicle discovery returned a vehicle without an id")
            try:
                vehicle_id = UUID(raw_id)
            except ValueError as exc:
                raise VehicleDiscoveryError(
                    f"vehicle discovery returned an invalid UUID: {raw_id!r}"
                ) from exc
            if vehicle_id not in vehicle_ids:
                vehicle_ids.append(vehicle_id)

        pages = payload.get("pages", 1)
        if not isinstance(pages, int) or pages < 0:
            raise VehicleDiscoveryError("vehicle discovery returned an invalid page count")
        if page >= pages:
            break
        page += 1

    return vehicle_ids
