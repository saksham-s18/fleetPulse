"""Logging configuration for the application and Uvicorn."""

import logging
import sys

LOG_FORMAT = "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s"
DATE_FORMAT = "%Y-%m-%d %H:%M:%S"

_UVICORN_LOGGERS = ("uvicorn", "uvicorn.error", "uvicorn.access")


def configure_logging(level: str = "INFO") -> None:
    """Configure root logging with a consistent format and the given level.

    ``force=True`` makes the function safe to call more than once, which is
    useful when the application factory is invoked in tests.
    """
    resolved_level = logging.getLevelNamesMapping().get(level.upper(), logging.INFO)

    logging.basicConfig(
        level=resolved_level,
        format=LOG_FORMAT,
        datefmt=DATE_FORMAT,
        stream=sys.stdout,
        force=True,
    )

    # Let Uvicorn's loggers use the same handlers and format as the application.
    for logger_name in _UVICORN_LOGGERS:
        uvicorn_logger = logging.getLogger(logger_name)
        uvicorn_logger.handlers.clear()
        uvicorn_logger.propagate = True
