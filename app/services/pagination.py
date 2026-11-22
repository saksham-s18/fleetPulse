"""Pagination helpers shared by list services."""

from app.schemas.common import Page


def build_page[T](items: list[T], total: int, page: int, page_size: int) -> Page[T]:
    """Build a response page and derive the total page count."""
    pages = (total + page_size - 1) // page_size if total else 0
    return Page(items=items, total=total, page=page, page_size=page_size, pages=pages)
