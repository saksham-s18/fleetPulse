"""Shared pytest configuration for the whole test suite.

Kafka is disabled by default so the ordinary unit suite never needs a
running broker: the lifespan then installs an ``UnavailableGpsEventPublisher``
and integration tests inject their own publisher double via dependency
override. Tests that exercise the real broker (``-m kafka``) build their own
``Settings`` and therefore opt back in explicitly.
"""

import os

os.environ.setdefault("KAFKA_ENABLED", "false")
# Redis is likewise disabled for the ordinary suite: the lifespan skips the
# readiness ping, so unit tests never require a running Redis server.
os.environ.setdefault("REDIS_ENABLED", "false")
