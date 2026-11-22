"""Command-line entry point for the FleetPulse GPS simulator."""

import argparse
import asyncio
import logging
import signal
from collections.abc import Callable, Sequence
from typing import Any
from uuid import UUID

from app.core.logging import configure_logging
from simulator.generator import GpsEventGenerator
from simulator.runner import SimulationRunner
from simulator.transport import DryRunPublisher, EventPublisher, HttpEventPublisher
from simulator.vehicles import VehicleDiscoveryError, fetch_active_vehicle_ids

logger = logging.getLogger(__name__)

LOG_LEVELS = ("DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL")
DEFAULT_API_URL = "http://127.0.0.1:8000"
DEFAULT_INGESTION_URL = "http://127.0.0.1:8000/api/v1/gps/events"


def build_parser() -> argparse.ArgumentParser:
    """Build the simulator argument parser."""
    parser = argparse.ArgumentParser(
        prog="python -m simulator",
        description=(
            "Generate GPS events for active FleetPulse vehicles and publish them "
            "to a configurable HTTP ingestion endpoint."
        ),
    )
    parser.add_argument(
        "--api-url",
        default=DEFAULT_API_URL,
        help=f"FleetPulse API used to discover active vehicles (default: {DEFAULT_API_URL})",
    )
    parser.add_argument(
        "--ingestion-url",
        default=DEFAULT_INGESTION_URL,
        help=f"HTTP endpoint that receives GPS events (default: {DEFAULT_INGESTION_URL})",
    )
    parser.add_argument(
        "--vehicles",
        type=int,
        default=5,
        metavar="COUNT",
        help="maximum number of active vehicles to simulate (default: 5)",
    )
    parser.add_argument(
        "--interval",
        type=float,
        default=1.0,
        dest="interval_seconds",
        metavar="SECONDS",
        help="seconds between GPS updates (default: 1.0)",
    )
    parser.add_argument(
        "--duration",
        type=float,
        default=60.0,
        dest="duration_seconds",
        metavar="SECONDS",
        help="total run time; 0 runs until interrupted (default: 60)",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=None,
        help="random seed for reproducible movement, speed and heading",
    )
    parser.add_argument(
        "--vehicle-id",
        type=UUID,
        action="append",
        dest="vehicle_ids",
        metavar="UUID",
        help="simulate this vehicle ID instead of discovering active vehicles; may be repeated",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=10.0,
        dest="timeout_seconds",
        metavar="SECONDS",
        help="HTTP timeout for discovery and publishing (default: 10)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="log generated events instead of sending HTTP requests",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=LOG_LEVELS,
        help="logging verbosity (default: INFO)",
    )
    return parser


def _install_shutdown_handlers(stop_event: asyncio.Event) -> Callable[[], None]:
    """Request a graceful stop on SIGINT or SIGTERM.

    ``loop.add_signal_handler`` is unavailable on Windows, so the function falls
    back to the :mod:`signal` module. The returned callable restores every
    handler it changed.
    """
    loop = asyncio.get_running_loop()
    loop_handlers: list[signal.Signals] = []
    fallback_handlers: dict[signal.Signals, Any] = {}

    for signal_name in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(signal_name, stop_event.set)
        except (NotImplementedError, RuntimeError, ValueError):
            try:
                fallback_handlers[signal_name] = signal.getsignal(signal_name)
                signal.signal(
                    signal_name,
                    lambda *_: loop.call_soon_threadsafe(stop_event.set),
                )
            except (OSError, ValueError):
                logger.debug("Could not install a handler for %s", signal_name)
        else:
            loop_handlers.append(signal_name)

    def restore() -> None:
        for signal_name in loop_handlers:
            loop.remove_signal_handler(signal_name)
        for signal_name, handler in fallback_handlers.items():
            signal.signal(signal_name, handler)

    return restore


async def _run(args: argparse.Namespace) -> int:
    if args.vehicles < 1:
        raise ValueError("--vehicles must be at least 1")
    if args.interval_seconds <= 0:
        raise ValueError("--interval must be positive")
    if args.duration_seconds < 0:
        raise ValueError("--duration must not be negative")
    if args.timeout_seconds <= 0:
        raise ValueError("--timeout must be positive")

    if args.vehicle_ids:
        vehicle_ids = list(dict.fromkeys(args.vehicle_ids))
        logger.info("Using %d explicitly supplied vehicle ID(s)", len(vehicle_ids))
    else:
        vehicle_ids = await fetch_active_vehicle_ids(
            args.api_url,
            timeout_seconds=args.timeout_seconds,
        )
        logger.info(
            "Discovered %d active vehicle(s) through %s",
            len(vehicle_ids),
            args.api_url,
        )

    if not vehicle_ids:
        raise VehicleDiscoveryError(
            "no active vehicles were found; create vehicles first or pass --vehicle-id"
        )

    if len(vehicle_ids) > args.vehicles:
        vehicle_ids = vehicle_ids[: args.vehicles]
    elif len(vehicle_ids) < args.vehicles:
        logger.warning(
            "Requested %d vehicle(s) but only %d are available",
            args.vehicles,
            len(vehicle_ids),
        )

    generator = GpsEventGenerator(vehicle_ids, seed=args.seed)
    publisher: EventPublisher
    if args.dry_run:
        publisher = DryRunPublisher()
        logger.info("Dry run enabled: events will be logged, not sent")
    else:
        publisher = HttpEventPublisher(
            args.ingestion_url,
            timeout_seconds=args.timeout_seconds,
        )
        logger.info("Publishing GPS events to %s", args.ingestion_url)

    stop_event = asyncio.Event()
    restore_handlers = _install_shutdown_handlers(stop_event)
    try:
        runner = SimulationRunner(
            generator,
            publisher,
            interval_seconds=args.interval_seconds,
            duration_seconds=args.duration_seconds,
            stop_event=stop_event,
        )
        stats = await runner.run()
    finally:
        restore_handlers()

    logger.info(
        "Simulation finished: %d tick(s), %d event(s) sent, %d failed",
        stats.ticks,
        stats.events_sent,
        stats.events_failed,
    )
    return 0 if stats.events_failed == 0 else 1


def main(argv: Sequence[str] | None = None) -> int:
    """Parse arguments, run the simulator and return a process exit code."""
    args = build_parser().parse_args(argv)
    configure_logging(args.log_level)

    try:
        return asyncio.run(_run(args))
    except VehicleDiscoveryError as exc:
        logger.error("%s", exc)
        return 1
    except ValueError as exc:
        logger.error("Invalid simulator configuration: %s", exc)
        return 1
    except KeyboardInterrupt:
        logger.info("Simulator interrupted")
        return 130
