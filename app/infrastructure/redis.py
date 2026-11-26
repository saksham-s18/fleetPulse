"""Async Redis client lifecycle and readiness checks.

Redis is a rebuildable projection of live vehicle state, not the source of
truth. This module owns every connection concern so repositories and services
only see an injected ``redis.asyncio.Redis`` instance:

- one client (and one connection pool) per process, never per request or per
  GPS event;
- an explicit ping-based readiness check used by the FastAPI lifespan;
- clean shutdown through ``aclose()``;
- application-level exceptions that never carry the Redis URL or credentials.

The :class:`RedisClient` wrapper is intentionally independent from FastAPI so
the standalone GPS worker can create and close the same client without
importing the web layer.
"""

import logging
from typing import cast

from redis.asyncio import Redis
from redis.exceptions import RedisError

from app.core.config import Settings

logger = logging.getLogger(__name__)

# Revalidate pooled connections at most this often (seconds).
HEALTH_CHECK_INTERVAL_SECONDS = 30


class RedisUnavailableError(Exception):
    """Raised when Redis cannot be reached or does not answer a ping.

    Messages must stay safe for server-side logs: no URL, password or raw
    client internals.
    """


def create_redis_client(settings: Settings) -> Redis:
    """Create a lazily connecting async Redis client with a bounded pool.

    Creating the client does not open a socket; :func:`ensure_redis_ready` (or
    the first command) establishes the connection. The client is shared by all
    callers for the lifetime of the process.
    """
    return cast(
        Redis,
        Redis.from_url(
            settings.redis_url.get_secret_value(),
            encoding="utf-8",
            decode_responses=True,
            socket_connect_timeout=settings.redis_connect_timeout_seconds,
            socket_timeout=settings.redis_socket_timeout_seconds,
            max_connections=settings.redis_max_connections,
            health_check_interval=HEALTH_CHECK_INTERVAL_SECONDS,
        ),
    )


async def check_redis(client: Redis) -> bool:
    """Return ``True`` when Redis answers ``PING``.

    Failures are logged by exception type only, so connection strings and
    credentials never leak into logs or health responses.
    """
    try:
        return bool(await client.ping())
    except (RedisError, OSError, TimeoutError) as exc:
        logger.warning("Redis readiness check failed (%s)", type(exc).__name__)
        return False


async def ensure_redis_ready(client: Redis) -> None:
    """Ping Redis or raise :class:`RedisUnavailableError`.

    Used during startup so the application fails loudly instead of serving
    requests against an unreachable live-state store.
    """
    try:
        pong = await client.ping()
    except (RedisError, OSError, TimeoutError) as exc:
        raise RedisUnavailableError("Redis readiness check failed") from exc
    if not pong:
        raise RedisUnavailableError("Redis readiness check returned no response")


class RedisClient:
    """Own one async Redis client and its lifecycle.

    ``start`` performs the readiness check; ``close`` releases the pool and is
    safe to call more than once. The underlying client is exposed through
    :attr:`client` for repository construction.
    """

    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._client = create_redis_client(settings)
        self._closed = False

    @property
    def client(self) -> Redis:
        """The shared async Redis client."""
        return self._client

    async def start(self) -> None:
        """Verify connectivity; raise :class:`RedisUnavailableError` on failure."""
        if self._closed:
            raise RedisUnavailableError("Redis client is closed")
        await ensure_redis_ready(self._client)
        logger.info(
            "Redis client ready (prefix=%s, max_connections=%d)",
            self._settings.redis_key_prefix,
            self._settings.redis_max_connections,
        )

    async def close(self) -> None:
        """Close the client and its connection pool; safe to call repeatedly."""
        if self._closed:
            return
        self._closed = True
        try:
            await self._client.aclose()
        except (RedisError, OSError, TimeoutError):  # pragma: no cover - best effort
            logger.warning("Redis client stopped with errors")
