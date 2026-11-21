"""Pydantic response models for API metadata endpoints."""

from pydantic import BaseModel


class RootResponse(BaseModel):
    """Basic information about the running API."""

    name: str
    version: str
    environment: str
    docs_url: str | None = None
