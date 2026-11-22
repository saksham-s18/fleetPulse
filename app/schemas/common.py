"""Shared request/response schemas."""

from pydantic import BaseModel, ConfigDict, Field


class RequestModel(BaseModel):
    """Base model for request payloads with strict, normalised input."""

    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)


class Location(RequestModel):
    """A named geographic point used as a shipment origin or destination."""

    name: str = Field(min_length=1, max_length=200)
    latitude: float = Field(ge=-90, le=90)
    longitude: float = Field(ge=-180, le=180)


class Page[T](BaseModel):
    """A page of results plus pagination metadata."""

    items: list[T]
    total: int = Field(ge=0)
    page: int = Field(ge=1)
    page_size: int = Field(ge=1)
    pages: int = Field(ge=0)
