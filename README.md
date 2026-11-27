# FleetPulse

FleetPulse is a Real-Time Fleet and Delivery Intelligence Platform. The
repository currently contains ten milestones:

- **Milestone 1** — modular FastAPI application, Pydantic Settings, structured
  logging, health probes and a tested toolchain.
- **Milestone 2** — async PostgreSQL connectivity with SQLAlchemy 2.x, asyncpg,
  Alembic migrations and a Docker Compose development database.
- **Milestone 3** — core fleet domain models for vehicles, drivers, shipments
  and historical vehicle assignments.
- **Milestone 4** — versioned REST APIs, validation schemas, repositories,
  service-layer business rules, explicit shipment transitions and assignment
  concurrency protection.
- **Milestone 5A** — a validated GPS event contract and a standalone vehicle
  simulator with predefined routes, deterministic movement and HTTP publishing.
- **Milestone 5B** — a GPS ingestion endpoint (`POST /api/v1/locations`) that
  validates GPS events against the fleet domain.
- **Milestone 5C.1** — a KRaft-mode Apache Kafka broker in Docker Compose and a
  producer that publishes validated GPS events to Kafka before the API answers
  `202 Accepted`.
- **Milestone 5C.2** — a standalone consumer worker with manual offsets,
  in-memory deduplication, bounded retries and an explicit dead-letter topic.
- **Milestone 6.1** — Redis infrastructure and a live-location projection:
  shared async client lifecycle, namespaced latest-location hashes, a GEO
  index and an atomic out-of-order/idempotency contract.
- **Milestone 6.2** — Kafka consumer-to-Redis integration: every successfully
  consumed GPS event updates the live-location hash and GEO index through the
  existing `LiveLocationService`, with explicit applied/duplicate/stale
  handling and Redis failures routed through the existing retry/DLQ policy.
- **Milestone 6.3** — live-location REST APIs: single-vehicle lookup,
  deterministic pagination over the Redis GEO index, and nearby-vehicle
  search with unit conversion and optional stale filtering.

Authentication, durable GPS persistence, location history, geofencing and
Kubernetes manifests remain intentionally out of scope.

## Technology stack

| Concern            | Tool                                    |
| ------------------ | --------------------------------------- |
| Language           | Python 3.12                             |
| Web framework      | FastAPI                                 |
| Validation/models  | Pydantic v2 + Pydantic Settings         |
| Database           | PostgreSQL 16 (Docker Compose locally)  |
| ORM / database     | SQLAlchemy 2.x async API + asyncpg      |
| Migrations         | Alembic                                 |
| Live state         | Redis 7.4 (Docker Compose locally)      |
| Redis client       | redis-py asyncio (`redis.asyncio`)      |
| Message broker     | Apache Kafka 3.9 (KRaft, Docker Compose) |
| Kafka client       | aiokafka (async producer and consumer)  |
| ASGI server        | Uvicorn                                 |
| Linting/formatting | Ruff                                    |
| Static typing      | Mypy (strict)                           |
| Testing            | Pytest + pytest-asyncio + TestClient    |
| GPS simulation     | Python asyncio + standard-library HTTP  |
| Project config     | `pyproject.toml`                        |

## Prerequisites

- Windows 10/11 with PowerShell 7+ (Windows PowerShell 5.1 also works)
- Python 3.12 installed and available as `py -3.12` or `python`
- Docker Desktop with Docker Compose v2
- Git

Optional: [uv](https://docs.astral.sh/uv/) for faster environment and dependency
management.

## Project layout

```text
fleetpulse/
|-- app/
|   |-- __init__.py
|   |-- main.py                  # App factory, lifespan, routers and handlers
|   |-- api/
|   |   |-- dependencies.py      # Session and service dependencies
|   |   |-- errors.py            # Consistent error envelope and handlers
|   |   `-- routes/
|   |       |-- assignments.py   # Assignment endpoints and history
|   |       |-- drivers.py       # Driver endpoints
|   |       |-- health.py        # GET /health/live, GET /health/ready
|   |       |-- locations.py     # GPS ingestion + live-location read endpoints
|   |       |-- root.py          # GET /
|   |       |-- shipments.py     # Shipment endpoints and transitions
|   |       `-- vehicles.py      # Vehicle endpoints
|   |-- core/
|   |   |-- config.py            # Pydantic Settings (app + database)
|   |   |-- exceptions.py        # Domain errors mapped to HTTP responses
|   |   `-- logging.py           # Logging setup
|   |-- infrastructure/
|   |   |-- __init__.py
|   |   |-- database.py          # Async engine, session factory, Base, probes
|   |   |-- kafka.py             # KafkaGpsEventPublisher and topic creation
|   |   |-- kafka_consumer.py    # KafkaGpsConsumer and DLQ transport
|   |   `-- redis.py             # Async Redis client lifecycle and readiness
|   |-- models/
|   |   |-- __init__.py          # Central model registration for Alembic
|   |   |-- enums.py             # Vehicle, driver and shipment statuses
|   |   |-- vehicle.py
|   |   |-- driver.py
|   |   |-- shipment.py
|   |   `-- vehicle_assignment.py
|   |-- repositories/
|   |   |-- assignment.py        # Assignment queries and active checks
|   |   |-- driver.py
|   |   |-- errors.py            # Integrity-error translation
|   |   |-- live_location.py     # Redis hashes, GEO index and atomic update
|   |   |-- shipment.py
|   |   `-- vehicle.py
|   |-- schemas/
|   |   |-- assignment.py
|   |   |-- common.py            # Location, request base and Page[T]
|   |   |-- driver.py
|   |   |-- errors.py
|   |   |-- gps.py               # Broker-independent GpsEvent contract
|   |   |-- health.py
|   |   |-- live_location.py     # Latest-location and GEO result models
|   |   |-- shipment.py
|   |   |-- system.py
|   |   `-- vehicle.py
|   |-- services/
|   |   |-- assignment.py        # Assignment business rules
|   |   |-- driver.py
|   |   |-- gps.py               # GPS ingestion: validate, then publish
|   |   |-- gps_consumer.py      # Dedup, retries and quarantine pipeline
|   |   |-- gps_publisher.py     # GpsEventPublisher protocol and errors
|   |   |-- live_location.py     # Validation, mapping and staleness
|   |   |-- live_location_handler.py # GPS event → Redis projection handler
|   |   |-- pagination.py
|   |   |-- shipment.py
|   |   |-- shipment_transitions.py
|   |   |-- transactions.py      # Commit/rollback and integrity translation
|   |   `-- vehicle.py
|   `-- workers/
|       |-- __init__.py
|       `-- gps_worker.py        # python -m app.workers.gps_worker
|-- alembic/
|   |-- env.py                   # Async migration environment
|   |-- script.py.mako
|   `-- versions/                # Migration revisions (baseline included)
|-- tests/
|   |-- test_api_errors.py       # Error envelope and OpenAPI tests
|   |-- test_database.py         # Unit tests (no PostgreSQL required)
|   |-- test_gps_ingestion_api.py # GPS endpoint wiring (no PostgreSQL)
|   |-- test_gps_consumer_service.py # Dedup, retries and quarantine logic
|   |-- test_gps_live_location_handler.py # Consumer→Redis outcomes and failures
|   |-- test_gps_publish_service.py # Publish ordering and 503 mapping
|   |-- test_gps_schema.py       # GPS contract validation tests
|   |-- test_gps_worker_redis.py # Worker Redis/Kafka lifecycle (no broker)
|   |-- test_health.py           # Unit tests (no PostgreSQL required)
|   |-- test_integrity_errors.py # Constraint translation tests
|   |-- test_kafka_consumer_infra.py # Offset commits and DLQ envelopes (mocked)
|   |-- test_kafka_publisher.py  # Producer config, keying, failures (mocked)
|   |-- test_lifespan_kafka.py   # Producer startup/shutdown lifecycle
|   |-- test_lifespan_redis.py   # Redis client startup/shutdown lifecycle
|   |-- test_live_location.py    # Live-location unit tests (fakeredis)
|   |-- test_live_location_api.py # Read API tests (fakeredis, no server)
|   |-- test_models.py           # Model metadata tests (no PostgreSQL required)
|   |-- test_redis_infrastructure.py # Redis client configuration and readiness
|   |-- test_schemas.py          # Request/response validation tests
|   |-- test_shipment_transitions.py
|   |-- test_simulator.py        # Movement, transport and shutdown tests
|   `-- integration/
|       |-- test_api.py          # REST API tests against real PostgreSQL
|       |-- test_assignment_concurrency.py
|       |-- test_database.py     # Real PostgreSQL integration tests
|       |-- test_gps_ingestion.py # GPS ingestion rules against PostgreSQL
|       |-- test_gps_consumer_kafka.py # Consumer, offsets and DLQ (marked kafka)
|       |-- test_gps_consumer_redis.py # Consumer→Redis flow (marked kafka_redis)
|       |-- test_gps_kafka_broker.py # Real broker round trip (marked kafka)
|       |-- test_gps_publish_api.py  # 202/503 publication semantics
|       |-- test_live_location_redis.py # Real Redis hashes, GEO, paging and freshness
|       |-- test_live_location_api_redis.py # Read API smoke tests against Redis
|       `-- test_models.py       # Constraints and relationship tests
|-- simulator/
|   |-- __init__.py
|   |-- __main__.py              # python -m simulator
|   |-- cli.py                   # Arguments, discovery and signal handling
|   |-- generator.py             # Deterministic event generation
|   |-- geo.py                   # Haversine distance and bearing helpers
|   |-- routes.py                # Predefined routes and interpolation
|   |-- runner.py                # Tick loop, duration and graceful shutdown
|   |-- transport.py             # Publisher protocol, HTTP and dry-run transports
|   `-- vehicles.py              # Active-vehicle discovery via the REST API
|-- alembic.ini
|-- compose.yaml
|-- .env.example
|-- .gitignore
|-- pyproject.toml
`-- README.md
```

## Configuration

Configuration is read from environment variables, then from an optional `.env`
file. Environment variables take precedence. `DATABASE_URL` and `REDIS_URL` are
stored as `SecretStr`, so their values (including any password) are masked in
logs and representations.

| Variable                   | Default                                                  | Notes                                      |
| -------------------------- | -------------------------------------------------------- | ------------------------------------------ |
| `APP_NAME`                 | `FleetPulse API`                                         | FastAPI title                              |
| `APP_VERSION`              | `0.1.0`                                                  | Reported by the API and OpenAPI            |
| `APP_ENV`                  | `development`                                            | `development`, `staging`, `production`, `test` |
| `LOG_LEVEL`                | `INFO`                                                   | `DEBUG` … `CRITICAL`                       |
| `DATABASE_URL`             | `postgresql+asyncpg://fleetpulse@localhost:5432/fleetpulse` | Async DSN; set a password in `.env`     |
| `DB_POOL_SIZE`             | `5`                                                      | Persistent connection pool size            |
| `DB_MAX_OVERFLOW`          | `10`                                                     | Extra connections above the pool size      |
| `DB_POOL_TIMEOUT_SECONDS`  | `30`                                                     | Wait time for a pooled connection          |
| `DB_CONNECT_TIMEOUT_SECONDS` | `5`                                                    | Database connect timeout                   |
| `POSTGRES_DB`              | `fleetpulse`                                             | Docker Compose only                        |
| `POSTGRES_USER`            | `fleetpulse`                                             | Docker Compose only                        |
| `POSTGRES_PASSWORD`        | *(required by Compose)*                                  | Local development value only               |
| `POSTGRES_PORT`            | `5432`                                                   | Host port mapped to the container          |
| `KAFKA_ENABLED`            | `true`                                                   | When `true`, Kafka is required at startup; GPS ingestion publishes events. `false` runs without a broker (unit tests) and makes ingestion answer `503` |
| `KAFKA_PORT`               | `9092`                                                   | Host port mapped to the Kafka broker (Compose only) |
| `KAFKA_BOOTSTRAP_SERVERS`  | `localhost:9092`                                         | Broker address used by the GPS producer    |
| `KAFKA_GPS_TOPIC`          | `gps.events`                                             | GPS events topic (created explicitly at startup) |
| `KAFKA_GPS_PARTITIONS`     | `3`                                                      | Partition count for the GPS topic          |
| `KAFKA_GPS_REPLICATION_FACTOR` | `1`                                                   | Single-broker local replication factor     |
| `KAFKA_DELIVERY_TIMEOUT_SECONDS` | `30`                                                 | Upper bound for waiting on broker acknowledgement |
| `KAFKA_CONSUMER_GROUP_ID`  | `fleetpulse-gps-processors`                              | Consumer group used by the GPS worker      |
| `KAFKA_GPS_DLQ_TOPIC`      | `gps.events.dlq`                                         | Dead-letter topic (created explicitly)     |
| `KAFKA_MAX_PROCESSING_ATTEMPTS` | `3`                                                  | Handler attempts before dead-lettering     |
| `KAFKA_RETRY_DELAY_SECONDS` | `1.0`                                                   | Delay between handler attempts             |
| `KAFKA_CONSUMER_POLL_TIMEOUT_SECONDS` | `1.0`                                          | Blocking time per poll when no records arrive |
| `REDIS_ENABLED`            | `true`                                                   | API pings Redis at startup; the GPS worker always requires Redis and rejects `false` |
| `REDIS_PORT`               | `6379`                                                   | Host port mapped to the Redis container (Compose only) |
| `REDIS_URL`                | `redis://localhost:6379/0`                               | Async Redis URL; stored as `SecretStr`      |
| `REDIS_CONNECT_TIMEOUT_SECONDS` | `2`                                                  | TCP connect timeout                         |
| `REDIS_SOCKET_TIMEOUT_SECONDS` | `2`                                                   | Command socket timeout                      |
| `REDIS_MAX_CONNECTIONS`    | `20`                                                     | Maximum size of the shared connection pool  |
| `REDIS_KEY_PREFIX`         | `fleetpulse`                                             | Namespace for every live-location key        |
| `REDIS_LOCATION_STALE_AFTER_SECONDS` | `60`                                            | Age after which a location is marked stale   |
| `REDIS_LOCATION_MAX_PAGE_SIZE` | `100`                                                  | Maximum `page_size` for live-location lists  |
| `REDIS_LOCATION_NEARBY_MAX_RADIUS_KM` | `100`                                             | Maximum nearby search radius in kilometres   |
| `REDIS_LOCATION_NEARBY_MAX_LIMIT` | `500`                                                 | Maximum nearby result limit                  |

OpenAPI documentation (`/docs`, `/redoc`, `/openapi.json`) is enabled only when
`APP_ENV=development`.

Create your local environment file:

```powershell
Copy-Item .env.example .env
```

The example contains local-development credentials only. Never commit `.env`
or real secrets; it is already excluded by `.gitignore`.

## Local PostgreSQL, Kafka and Redis with Docker Compose

Start the development services:

```powershell
docker compose up -d postgres kafka redis
docker compose ps
```

Wait until all services report `healthy` (the compose health checks use
`pg_isready`, `kafka-broker-api-versions.sh` and `redis-cli ping`).

**PostgreSQL** stores fleet data in the named volume
`fleetpulse_fleetpulse_postgres_data`.

**Kafka** runs as a single KRaft broker (no ZooKeeper) from the pinned
`apache/kafka:3.9.0` image, with the combined broker+controller role, a
PLAINTEXT listener on `localhost:${KAFKA_PORT:-9092}` and persistent storage in
the named volume `fleetpulse_fleetpulse_kafka_data`. Broker auto-topic creation
is disabled (`KAFKA_AUTO_CREATE_TOPICS_ENABLE=false`): the application creates
its GPS topic explicitly at startup.

**Redis** runs from the pinned `redis:7.4.2-alpine` image on
`localhost:${REDIS_PORT:-6379}`, with a persistent named volume
`fleetpulse_fleetpulse_redis_data`. AOF (`appendfsync everysec`) and RDB
snapshots (`save 60 1`) are enabled. Memory is capped at 256 MB with
`maxmemory-policy noeviction`: live vehicle positions are never silently
evicted; when the limit is reached Redis rejects writes instead. Redis is a
rebuildable projection, not the source of truth — see "Redis persistence and
recovery" for what survives a restart or volume loss.

Follow logs or manage the services:

```powershell
docker compose logs -f postgres kafka redis
docker compose stop kafka        # stop, keep container and data
docker compose start kafka       # start again
docker compose down              # remove containers and network, keep data
docker compose down -v           # remove containers AND volumes (destructive)
```

## Setup (Windows PowerShell)

From the repository root:

```powershell
# Create and activate a Python 3.12 virtual environment
py -3.12 -m venv .venv
.\.venv\Scripts\Activate.ps1

# If script execution is blocked, allow it for this PowerShell session only:
# Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass

# Install the project with development dependencies
python -m pip install --upgrade pip
pip install -e ".[dev]"

# Create local configuration
Copy-Item .env.example .env
```

### Setup with uv (optional)

```powershell
uv venv --python 3.12
uv pip install -e ".[dev]"
Copy-Item .env.example .env
```

## Database migrations

Alembic reads `DATABASE_URL` through the application settings. No credentials
are stored in `alembic.ini`. The declarative `Base` uses a stable constraint
naming convention and is ready for SQLAlchemy 2.x typed models.

```powershell
alembic current
alembic upgrade head
alembic revision --autogenerate -m "describe your change"
alembic downgrade -1
alembic history
```

The baseline revision `05a337c43e7f` creates the four core tables. Alembic
imports `app.models`, whose `__init__.py` is the central registration point, so
new model modules must be imported there before autogenerating a migration.

## Domain models

| Entity              | Key fields and constraints                                                        |
| ------------------- | --------------------------------------------------------------------------------- |
| `Vehicle`           | UUID id, unique registration number, unique optional device id, status check      |
| `Driver`            | UUID id, unique license number, unique optional phone, status check               |
| `Shipment`          | UUID id, unique reference, JSONB origin/destination, status check                 |
| `VehicleAssignment` | UUID id, vehicle/driver/shipment foreign keys, assigned/ended timestamps           |

Database-level guarantees:

- Status columns are `VARCHAR(32)` with named `CHECK` constraints for allowed values.
- `vehicle_assignments.ended_at` must be `NULL` or greater than or equal to
  `assigned_at`.
- Three PostgreSQL partial unique indexes prevent more than one active
  (`ended_at IS NULL`) assignment per vehicle, driver and shipment.
- Foreign keys use `ON DELETE RESTRICT`, and parent relationships use
  `passive_deletes=True`, so vehicles, drivers and shipments with assignment
  history cannot be deleted and history is never cascaded away.
- Non-unique indexes on the three assignment foreign keys support historical
  lookups; the partial unique indexes cover the active subset.

Timestamps use PostgreSQL `TIMESTAMP WITH TIME ZONE`. Model classes use
SQLAlchemy 2.x typed `Mapped[...]` / `mapped_column(...)` declarations.

## GPS event contract

`app/schemas/gps.py` defines `GpsEvent`, the validated payload shared by the
simulator and future ingestion components. It imports nothing from Kafka,
SQLAlchemy or the web layer, so the same contract can be used by a device
gateway, an HTTP endpoint or a message consumer.

| Field            | Type             | Rules                                                        |
| ---------------- | ---------------- | ------------------------------------------------------------ |
| `event_id`       | UUID             | Required; unique per observation                              |
| `vehicle_id`     | UUID             | Required; identifies an existing FleetPulse vehicle            |
| `shipment_id`    | UUID or `null`   | Optional; the contract does not enforce a foreign key          |
| `latitude`       | float            | Required; `-90` through `90` inclusive; NaN and infinity rejected |
| `longitude`      | float            | Required; `-180` through `180` inclusive; NaN and infinity rejected |
| `speed_kmph`     | float            | Required; `>= 0`; NaN and infinity rejected                     |
| `heading`        | float or `null`  | Optional; `0` through `360` inclusive; `360` is north          |
| `recorded_at`    | datetime         | Required; must be timezone-aware and is normalised to UTC      |
| `schema_version` | string           | Defaults to `1.0`; 1–32 characters                             |

Example payload:

```json
{
  "event_id": "6f1c2b1e-9f7a-4b1a-9c2d-3a4b5c6d7e8f",
  "vehicle_id": "0b6f2c9a-1d2e-4f3a-8b7c-9d0e1f2a3b4c",
  "shipment_id": null,
  "latitude": 52.5219,
  "longitude": 13.4132,
  "speed_kmph": 32.4,
  "heading": 87.5,
  "recorded_at": "2026-01-01T12:00:00Z",
  "schema_version": "1.0"
}
```

Validation behaviour:

- Naive timestamps such as `2026-01-01T12:00:00` are rejected. Offset-aware
  values are converted to UTC, so `14:00+02:00` becomes `12:00Z`.
- Coordinates, speed and heading are checked inclusively at their boundaries.
  NaN and infinite floats are rejected rather than silently persisted.
- Unknown fields are rejected, matching the strict request-model convention.
- `GpsEvent.model_dump(mode="json")` produces a JSON-safe document with string
  UUIDs and ISO-8601 timestamps.

## Vehicle GPS simulator

`simulator/` is a standalone asyncio program that generates GPS updates for
existing FleetPulse vehicles. It needs no physical device and no database
connection of its own: it discovers active vehicles through the REST API (or
accepts explicit IDs) and publishes events over a pluggable transport.

Inspect the options:

```powershell
python -m simulator --help
```

Generate and log events without sending them:

```powershell
python -m simulator --dry-run --vehicles 3 --interval 1 --duration 30 --seed 42
```

Publish to the HTTP ingestion endpoint (the default URL):

```powershell
python -m simulator --vehicles 5 --interval 2 --duration 120
```

Or point it explicitly:

```powershell
python -m simulator --ingestion-url http://127.0.0.1:8000/api/v1/locations `
  --vehicles 5 --interval 2 --duration 120
```

Simulate one known vehicle until interrupted:

```powershell
python -m simulator --dry-run --vehicle-id 0b6f2c9a-1d2e-4f3a-8b7c-9d0e1f2a3b4c `
  --duration 0
```

| Option            | Default                                  | Purpose                                              |
| ----------------- | ---------------------------------------- | ---------------------------------------------------- |
| `--api-url`       | `http://127.0.0.1:8000`                  | FleetPulse API used to discover active vehicles       |
| `--ingestion-url` | `http://127.0.0.1:8000/api/v1/locations`  | HTTP endpoint that receives GPS events               |
| `--vehicles`      | `5`                                      | Maximum number of active vehicles to simulate        |
| `--interval`      | `1.0`                                    | Seconds between GPS updates                          |
| `--duration`      | `60`                                     | Total run time; `0` runs until interrupted           |
| `--seed`          | none                                     | Random seed for reproducible movement                |
| `--vehicle-id`    | none                                     | Repeatable explicit UUID; bypasses API discovery     |
| `--timeout`       | `10`                                     | HTTP timeout for discovery and publishing            |
| `--dry-run`       | off                                      | Log events instead of sending HTTP requests          |
| `--log-level`     | `INFO`                                   | `DEBUG` through `CRITICAL`                           |

How the simulation works:

- Active vehicles are loaded from
  `GET /api/v1/vehicles?status=active&page_size=100`, following API pagination.
  If fewer vehicles exist than `--vehicles`, all available vehicles are used
  with a warning; if none exist, the simulator exits with status `1`.
- Every vehicle is assigned a closed route from `simulator/routes.py`. Routes
  are shuffled with the seed, so a seed also reproduces route assignment.
- Each tick advances every vehicle by `speed × elapsed time` along its route.
  Speed is smoothed inside the route's realistic range, and heading is the
  compass bearing to the next waypoint.
- One event is emitted per vehicle per tick. Event IDs are random UUID4 values,
  and timestamps are timezone-aware UTC.
- A seed reproduces coordinates, speeds, headings, timestamps and route
  assignment for the same vehicle order and tick schedule. Event IDs are
  intentionally **not** reproducible so repeated runs do not reuse IDs.
- `SIGINT` and `SIGTERM` request a graceful stop. The current tick finishes, the
  publisher is closed, and a summary is logged. On Windows the simulator falls
  back to `signal.signal` because `loop.add_signal_handler` is unavailable.
- Publishing is isolated behind the `EventPublisher` protocol.
  `HttpEventPublisher` uses the standard library, while `DryRunPublisher` logs
  events. A future Kafka publisher can replace them without changing the
  generator or the tick loop.
- Transport failures are logged and counted. The process exits with status `1`
  if any event could not be published.

Known limitations as of Milestone 5C.1:

- The HTTP ingestion endpoint validates an event and publishes it to Kafka
  before answering `202`, but nothing consumes the topic yet and no GPS rows
  are stored. A `202` is a broker acknowledgement, not a durability guarantee
  in PostgreSQL (see "GPS ingestion" below); when Kafka is unavailable the
  endpoint answers `503`, which the simulator counts as a failed publication.
- There are no retries, backoff, batching, authentication or TLS client
  settings in the HTTP transport.
- Vehicle discovery requires a running API and database. `--vehicle-id` allows
  the simulator to run without them.
- The simulator is a repository tool and is not part of the installed wheel
  (`packages = ["app"]`); run it from the repository root.

## Core fleet API

All business endpoints are versioned under `/api/v1`. List endpoints accept
`page` (default `1`) and `page_size` (default `20`, maximum `100`) and return
`items`, `total`, `page`, `page_size` and `pages`.

| Method | Path                                      | Purpose                                      |
| ------ | ----------------------------------------- | -------------------------------------------- |
| POST   | `/api/v1/vehicles`                        | Create a vehicle                             |
| GET    | `/api/v1/vehicles`                        | List vehicles; filter `status`, `vehicle_type` |
| GET    | `/api/v1/vehicles/{vehicle_id}`           | Get a vehicle                                |
| PATCH  | `/api/v1/vehicles/{vehicle_id}`           | Update a vehicle                             |
| PATCH  | `/api/v1/vehicles/{vehicle_id}/status`    | Change vehicle status                        |
| POST   | `/api/v1/drivers`                         | Create a driver                              |
| GET    | `/api/v1/drivers`                         | List drivers; filter `status`                |
| GET    | `/api/v1/drivers/{driver_id}`             | Get a driver                                 |
| PATCH  | `/api/v1/drivers/{driver_id}`             | Update a driver                              |
| PATCH  | `/api/v1/drivers/{driver_id}/status`      | Change driver status                         |
| POST   | `/api/v1/shipments`                       | Create a shipment                            |
| GET    | `/api/v1/shipments`                       | List shipments; filter `status`              |
| GET    | `/api/v1/shipments/{shipment_id}`         | Get a shipment                               |
| PATCH  | `/api/v1/shipments/{shipment_id}`         | Update a shipment                            |
| PATCH  | `/api/v1/shipments/{shipment_id}/status`  | Apply an allowed status transition           |
| POST   | `/api/v1/assignments`                     | Assign a vehicle and driver to a shipment    |
| GET    | `/api/v1/assignments`                     | List assignments; filter IDs and `active`    |
| GET    | `/api/v1/assignments/{assignment_id}`     | Get an assignment                            |
| POST   | `/api/v1/assignments/{assignment_id}/end` | End an assignment and preserve history       |
| GET    | `/api/v1/vehicles/{vehicle_id}/assignments` | Vehicle assignment history                 |
| GET    | `/api/v1/drivers/{driver_id}/assignments` | Driver assignment history                    |
| GET    | `/api/v1/shipments/{shipment_id}/assignments` | Shipment assignment history              |
| POST   | `/api/v1/locations`                     | Publish a validated GPS event to Kafka    |
| GET    | `/api/v1/locations`                     | List live locations; filter `is_stale`, `vehicle_id` |
| GET    | `/api/v1/locations/{vehicle_id}`        | Get one vehicle's latest live location    |
| GET    | `/api/v1/locations/nearby`              | Find vehicles near a point via Redis GEO  |

Shipment status transitions are explicit:

| Current      | Allowed targets             |
| ------------ | --------------------------- |
| `created`    | `assigned`, `cancelled`     |
| `assigned`   | `in_transit`, `cancelled`   |
| `in_transit` | `delivered`, `cancelled`    |
| `delivered`  | none                        |
| `cancelled`  | none                        |

Assignment rules:

- The vehicle and driver must exist and be `active`.
- The shipment must be in `created` status.
- A vehicle, driver and shipment may each have at most one active assignment.
  PostgreSQL partial unique indexes are the final protection for concurrent
  requests.
- Creating an assignment and changing the shipment to `assigned` happen in one
  transaction. Ending an assignment sets `ended_at`, keeps the row as history,
  and releases an `assigned` shipment back to `created`.
- Ending an assignment twice returns `409 Conflict`.
- No delete endpoints are exposed. Historical assignments cannot be removed,
  and `ON DELETE RESTRICT` protects vehicles, drivers and shipments at the
  database level.

### Error responses

Every error uses one envelope and never exposes SQL, credentials or internal
exception details:

```json
{
  "error": {
    "code": "conflict",
    "message": "The vehicle already has an active assignment.",
    "details": null
  }
}
```

| Status | Code               | Typical cause                              |
| ------ | ------------------ | ------------------------------------------ |
| 400    | `bad_request`      | Empty PATCH payload or data constraint     |
| 404    | `not_found`        | Unknown entity or route                    |
| 409    | `conflict`         | Duplicate value, active assignment or invalid transition |
| 422    | `validation_error` | Schema or query validation failure         |
| 500    | `internal_error`   | Unexpected server failure                  |
| 503    | `service_unavailable` | Kafka delivery failed or is disabled    |

## GPS ingestion

`POST /api/v1/locations` accepts the `GpsEvent` contract described above. The
route handler only parses the payload with the existing `GpsEvent` schema;
all business rules live in `app/services/gps.py` (`GpsIngestService`), which
publishes validated events through the `GpsEventPublisher` protocol
(`app/services/gps_publisher.py`).

| Condition                                                     | Status | Error code           |
| ------------------------------------------------------------- | ------ | -------------------- |
| Valid event, published to Kafka                               | `202`  | —                    |
| Kafka delivery failed or Kafka disabled                       | `503`  | `service_unavailable` |
| Unknown `vehicle_id`                                          | `404`  | `not_found`          |
| Vehicle exists but is not `active`                            | `409`  | `conflict`           |
| Unknown `shipment_id`                                         | `404`  | `not_found`          |
| Shipment is not on the vehicle's current active assignment     | `409`  | `conflict`           |
| Payload fails `GpsEvent` validation                           | `422`  | `validation_error`   |

Behaviour (order matters):

1. Validate the payload against the `GpsEvent` schema.
2. Verify the vehicle exists and is `active`.
3. If the event references a `shipment_id`, verify the shipment exists and sits
   on the vehicle's **current** active assignment (`ended_at IS NULL`); events
   referencing a past assignment are rejected.
4. Publish the event to Kafka and wait for the broker acknowledgement.
5. Return `202 Accepted` only after the acknowledgement; never before.

A successful response is:

```json
{
  "event_id": "6f1c2b1e-9f7a-4b1a-9c2d-3a4b5c6d7e8f",
  "vehicle_id": "0b6f2c9a-1d2e-4f3a-8b7c-9d0e1f2a3b4c",
  "shipment_id": null,
  "status": "accepted",
  "persisted": false
}
```

Validation failures never reach the publisher, and a delivery failure can never
produce `202`: it maps to the standard error envelope with `503` and
`service_unavailable`. The response contains no broker address, client error or
stack trace — those details stay in server-side logs.

**`persisted: false` remains accurate.** A Kafka acknowledgement means the
broker durably accepted the event under `acks=all`; it does *not* mean the
event is stored in PostgreSQL. No GPS table or outbox exists yet
(see "Kafka ingestion" below).

## Kafka ingestion (producer and consumer)

Kafka is the hand-off point between the HTTP API and the processing stages of
the platform: the API validates a GPS event and streams it into `gps.events`,
and the standalone worker consumes that topic and projects each handled event
into Redis live state. Geofencing, alerts, location APIs and durable GPS
storage belong to later milestones.

### Topic design

| Setting                   | Value / default            | Purpose                                    |
| ------------------------- | -------------------------- | ------------------------------------------ |
| `KAFKA_GPS_TOPIC`         | `gps.events`               | GPS events topic name                      |
| `KAFKA_GPS_PARTITIONS`    | `3`                        | Partition count (local-development default) |
| `KAFKA_GPS_REPLICATION_FACTOR` | `1`                    | Single-broker local environment            |
| Message key               | `vehicle_id` (string UUID) | Deterministic partition assignment         |

- The topic is created **explicitly at application startup** with the configured
  partitions and replication factor; broker auto-topic creation is disabled in
  Compose, so nothing depends on implicit broker behaviour.
- The message key is `vehicle_id`. Kafka's default partitioner hashes the key,
  so every event of one vehicle lands on the same partition and events for that
  vehicle keep their relative order. Events of different vehicles may interleave
  across partitions.
- The value is compact UTF-8 JSON produced from `GpsEvent.model_dump(mode="json")`
  and preserves `event_id`, `vehicle_id`, optional `shipment_id`, coordinates,
  speed, heading, `recorded_at` and `schema_version`.

### Producer lifecycle

- One `KafkaGpsEventPublisher` (`app/infrastructure/kafka.py`) is created and
  started in the FastAPI lifespan when `KAFKA_ENABLED=true`, stored on
  `app.state.gps_publisher` and shared by all requests through dependency
  injection; it is closed during shutdown. No producer is created per request
  or stored in a global module variable.
- The producer uses `acks=all` and `enable_idempotence=True`
  (aiokafka `AIOKafkaProducer`). aiokafka retries retriable errors indefinitely
  and has no delivery timeout, so `KAFKA_DELIVERY_TIMEOUT_SECONDS` caps the
  total wait for the acknowledgement; exceeding it fails the publication
  (`503`) instead of hanging the request.
- If Kafka is required but the producer cannot start, application startup fails
  with a clear error instead of serving an API that silently drops events.
- When `KAFKA_ENABLED=false` (the default for the unit suite), no broker is
  needed: an in-memory placeholder publisher rejects every publication, so
  GPS ingestion answers `503` instead of pretending acceptance.

### Acknowledgement is not persistence

A `202` response means: *validated, published, broker-acknowledged*. It does
not mean the event is queryable — there is no GPS table yet, and `persisted`
stays `false`. If the broker is unavailable at any point, the API answers
`503 service_unavailable` (the simulator counts it as a failed publication and
exits non-zero); requests are not retried, buffered or queued by the API.

### Current limitations

- The worker projects handled events into Redis and updates in-memory counters;
  it does not persist GPS events in PostgreSQL.
- GPS events are not persisted in PostgreSQL; `persisted` is always `false`.
- If the delivery timeout elapses, the API answers `503` (delivery not
  confirmed). The producer may still deliver that event once the broker
  recovers; idempotence prevents duplicates, but clients must treat `503` as
  "unknown", not "definitely not delivered".
- No retries, batching, compression, TLS/SASL or authentication are configured
  for the producer; the local broker is a single KRaft node with PLAINTEXT
  listeners.
- No schema registry: the JSON contract is versioned by `schema_version` only.

## Kafka GPS consumer worker

The consumer is an **independent process**, never started by the FastAPI
lifespan. Since Milestone 6.2 it also requires Redis:

```powershell
docker compose up -d kafka redis         # worker infrastructure
python -m app.workers.gps_worker         # consumes until Ctrl+C / SIGTERM
```

For the complete API → Kafka → worker → Redis flow, also start PostgreSQL and
the API (`uvicorn app.main:app`) plus the simulator. Useful options:

| Option              | Default | Purpose                                          |
| ------------------- | ------- | ------------------------------------------------ |
| `--duration-seconds` | `0`     | Stop gracefully after N seconds; `0` until stopped |
| `--log-level`       | settings `LOG_LEVEL` | `DEBUG` through `CRITICAL`      |

Architecture: `app/infrastructure/kafka_consumer.py` owns fetching, manual
offset commits and the DLQ producer; `app/services/gps_consumer.py` owns
validation, deduplication, retries and the handler boundary;
`app/services/live_location_handler.py` maps one validated event onto
`LiveLocationService.update_from_event`; `app/workers/gps_worker.py` wires the
transport, Redis client and handler together and handles signals. The worker
creates exactly one `RedisClient`/connection pool per process, pings it during
startup, and closes it on shutdown independently of the FastAPI lifespan. A
missing Redis is a startup failure (exit code `1`), not a per-message surprise.

### Event processing and Redis outcomes

For each validated event the worker:

1. checks the bounded in-memory deduplicator;
2. calls `LiveLocationService`, whose repository runs the atomic Redis Lua
   compare-and-set for the location hash and GEO index;
3. marks the event as processed only after Redis reports success;
4. lets the Kafka transport commit `record.offset + 1` afterwards.

| Result | Meaning | Kafka offset |
| ------ | ------- | ------------ |
| `applied` | A strictly newer observation was written to hash + GEO | Committed |
| `duplicate` (Redis) | The same `event_id` is already stored; nothing is written | Committed |
| `stale` | A newer `(recorded_at, event_id)` is already stored; nothing is written | Committed |
| `duplicate` (in-memory) | The event ID is still in the process-local deduplicator; handler is skipped | Committed |
| `redis_error` | Redis was unreachable, timed out or rejected the write (including OOM) | Not committed; retried, then DLQ |

Redis failures are translated to the retryable `GpsHandlerError` with
`failure_kind="redis"`; they are never classified as invalid GPS payloads.
They do not mark the event processed, do not commit the offset and do not
overwrite live state. If the bounded retry budget is exhausted, the original
payload is quarantined with `error.type="redis_error"`; the offset advances
only after the DLQ broker acknowledgement.

### Consumer group and ordering

- `KAFKA_CONSUMER_GROUP_ID` defaults to `fleetpulse-gps-processors`. Run one
  worker locally; multiple instances would share partitions through normal
  Kafka rebalancing (and would each need their own deduplication store).
- Messages are processed **one at a time**, and `max_poll_records=1` prevents
  concurrent handling inside a partition. Because producers key by
  `vehicle_id`, all events of a vehicle share a partition and keep their order.
  Concurrency across independent partitions can be added later.
- `KAFKA_GPS_TOPIC` and `KAFKA_GPS_DLQ_TOPIC` are created explicitly at worker
  startup with the configured partitions and replication factor.

### Manual offsets and at-least-once delivery

- Automatic commits are disabled (`enable_auto_commit=False`). The worker
  commits `record.offset + 1` for the record's partition only after the service
  reports success, an in-memory duplicate, or a dead-letter acknowledgement.
- A failed Redis update or a failed DLQ publication never commits: the worker
  logs the failure, exits non-zero after retries are exhausted, and Kafka
  redelivers the message on restart.
- Processing is therefore **at-least-once**, not exactly-once. A crash between
  the Redis write and the offset commit replays the event. The in-memory
  deduplicator may suppress it, but after a restart the atomic Redis
  compare-and-set is the final protection: the repeated event is reported as a
  duplicate, the position does not regress, and the offset can be committed.

### In-memory deduplication (non-durable)

- `InMemoryEventDeduplicator` tracks `event_id` values in a bounded FIFO
  (default 100,000) behind the replaceable `EventDeduplicator` protocol.
- An ID is marked **only after** the handler succeeds, so a failed Redis write
  is retried on redelivery instead of being skipped.
- The state is process-local: it does **not** survive worker restarts and is
  **not** shared between instances. After a restart (or FIFO eviction) a
  redelivered event reaches Redis again. Redis/PostgreSQL deduplication is
  intentionally out of scope because the atomic update already makes the
  projection idempotent per event.

### Retries and dead-letter handling

- Invalid JSON, an empty value, or a payload that fails `GpsEvent` validation
  is quarantined immediately (no handler call, no silent discard).
- Handler failures, including Redis unavailability, timeouts and rejected
  writes, are retried up to `KAFKA_MAX_PROCESSING_ATTEMPTS` (default `3`) with
  `KAFKA_RETRY_DELAY_SECONDS` (default `1.0`). Exhausted attempts are
  quarantined.
- Quarantine publishes to `KAFKA_GPS_DLQ_TOPIC` (default `gps.events.dlq`) and
  waits for the broker acknowledgement **before** the original offset is
  committed. If the DLQ publish fails, nothing is committed.
- The DLQ envelope carries `original_topic`, `original_partition`,
  `original_offset`, `original_timestamp_ms`, the original key (also used as
  the DLQ message key), `event_id` when known, the raw payload as UTF-8 or
  base64 (`payload_encoding`), and `error: {type, message}`. Redis failures use
  `error.type="redis_error"`; messages stay log-safe and never include the
  Redis URL or credentials.

### Statistics and logging

The worker summary distinguishes `received`, `processed`, `applied`,
`duplicates` (in-memory), `location_duplicates` (Redis), `stale`, `invalid`,
`handler_failures`, `redis_failures`, `retries` and `quarantined`. Logs include
the event ID and vehicle ID for handler outcomes; full GPS payloads are not
logged. Prometheus metrics are intentionally deferred to a later milestone.

### Shutdown and logging

`SIGINT` and `SIGTERM` set a stop event: the worker stops fetching, finishes
and commits the record in flight, closes the consumer, DLQ producer and Redis
client, and logs the counter summary. No consumer runs inside the API process,
and the worker is not a Compose service because the repository has no
application image (Compose provides PostgreSQL, Kafka and Redis; API, simulator
and worker run on the host).

## Redis live-location projection (Milestones 6.1–6.3)

Redis stores the current position of every reporting vehicle so later
milestones can answer "where is it now?" and "what is nearby?" without
scanning PostgreSQL. It is a **rebuildable projection**: PostgreSQL remains
the durable domain store and Kafka remains the event log. Redis is not the
source of truth.

Milestone 6.1 introduced the repository, service and atomic update contract.
Milestone 6.2 connects the standalone Kafka worker to
`LiveLocationService.update_from_event`. Milestone 6.3 adds the read endpoints
documented above. PostgreSQL GPS persistence still does not exist.

### Key naming and data structures

Every key is namespaced by `REDIS_KEY_PREFIX` (default `fleetpulse`):

| Key                                  | Type            | Purpose                                      |
| ------------------------------------ | --------------- | -------------------------------------------- |
| `{prefix}:live:location:{vehicle_id}` | hash           | Latest observation for one vehicle           |
| `{prefix}:live:geo`                   | GEO sorted set | Vehicle coordinates for radius queries       |

The latest-location hash fields are:

| Field            | Stored as                    | Notes                                   |
| ---------------- | ---------------------------- | --------------------------------------- |
| `vehicle_id`     | UUID string                  | Hash owner and GEO member               |
| `event_id`       | UUID string                  | Idempotency and tie-break key           |
| `latitude`       | decimal string               | `-90` … `90`                            |
| `longitude`      | decimal string               | `-180` … `180`                          |
| `speed_kmh`      | decimal string               | Maps `GpsEvent.speed_kmph`              |
| `heading`        | decimal string or `""`       | Empty string represents a missing value |
| `recorded_at`    | ISO-8601 UTC string          | Device observation time                 |
| `recorded_at_ms` | integer string               | Ordering field used by the Lua script   |
| `received_at`    | ISO-8601 UTC string          | When the projection accepted the event  |
| `received_at_ms` | integer string               | Numeric form of `received_at`           |
| `shipment_id`    | UUID string or `""`          | Optional shipment context               |

The hash deliberately has **no TTL**. A vehicle that stops sending events
keeps its last known position; freshness is calculated from timestamps (see
below) instead of deleting data.

The GEO sorted set uses the vehicle UUID as its member and stores
`(longitude, latitude)`. `GEOADD` adds or moves a vehicle, `GEOPOS` returns its
coordinates and `GEOSEARCH ... BYRADIUS` returns nearby vehicles ordered by
distance. A radius result is therefore backed by the same accepted update that
wrote the hash.

### Ordering, idempotency and atomicity

GPS events can be duplicated or delivered out of order. One Lua script performs
the whole update atomically:

1. Read the stored `recorded_at_ms` and `event_id`.
2. If the stored `recorded_at_ms` is greater, reject the event as **stale**.
3. If the timestamps are equal, compare the canonical UUID strings:
   a larger `event_id` wins; an equal ID is a **duplicate**.
4. Otherwise `HSET` the latest hash and `GEOADD` the new position in the same
   atomic script, then report **applied**.

Because the hash and GEO entry are written by one script, an accepted event
cannot leave the hash updated while the GEO index still points at the old
position. Redis executes the script atomically, so concurrent deliveries
converge on the highest `(recorded_at, event_id)` pair. Repeated delivery of
the same event changes nothing and is idempotent per event.

This is **at-least-once** processing with idempotent projection writes, not
durable exactly-once processing. A crash between Kafka handling and offset
commit can replay an event; the update script recognises the duplicate.

### Freshness and staleness

`recorded_at` is the freshness source. `LiveLocationService.get_location`
returns `stale=true` when `now - recorded_at` exceeds
`REDIS_LOCATION_STALE_AFTER_SECONDS` (default `60`) and reports `age_seconds`.
Future timestamps are treated as fresh with an age of zero. Staleness is a
computed response field, never a key TTL: the last known location remains
queryable so an operator can see where a silent vehicle was last seen.

### Client lifecycle

`app/infrastructure/redis.py` owns one async `redis.asyncio.Redis` client and
one bounded connection pool per process. The FastAPI lifespan creates it,
pings it when `REDIS_ENABLED=true`, stores it on `app.state.redis_client` and
closes it during shutdown. The standalone GPS worker creates its own
`RedisClient` during startup, pings it, and closes it during shutdown; it does
not depend on the FastAPI lifespan. `get_redis_client` and
`get_live_location_service` in `app/api/dependencies.py` inject the shared API
client. No connection is created per request, per message or per GPS event.
Startup fails loudly (without exposing the URL) when Redis is required but
unreachable. Unit tests set `REDIS_ENABLED=false` and never require a Redis
server.

### Redis persistence and recovery

The Compose service persists to `fleetpulse_fleetpulse_redis_data` using AOF
(everysec) plus RDB snapshots, so ordinary container restarts retain live
state. Those mechanisms are not a durability guarantee:

- AOF `everysec` can lose roughly the last second of accepted writes on a hard
  crash; RDB snapshots are point-in-time and can be older.
- Deleting the volume (`docker compose down -v`) or losing the host disk loses
  the projection entirely.
- The local deployment is a single Redis node with no replication or failover.
- Kafka currently retains GPS events according to broker defaults, which are
  not configured for indefinite replay. Redis cannot be guaranteed fully
  reconstructible until retention is explicitly sized for the rebuild window.
- No replay/rebuild worker exists yet. The live consumer projects new events
  into Redis, but there is no operational rebuild command or runbook.

A future rebuild process would clear the configured namespace, reset or seek
the consumer group to the earliest retained offset and replay `gps.events`
through the same idempotent update contract. Because updates are ordered by
`recorded_at` and `event_id`, replaying retained events in any order converges
on the same latest state. That worker, retention tuning and an operational
rebuild runbook are later milestones; this milestone only documents the path.

## Live-location read API (Milestone 6.3)

Three read endpoints expose the Redis projection. They never query
PostgreSQL for location data and never expose Redis keys, commands or
connection details.

| Method | Path                              | Purpose                                      |
| ------ | --------------------------------- | -------------------------------------------- |
| GET    | `/api/v1/locations/{vehicle_id}`  | Latest location for one vehicle              |
| GET    | `/api/v1/locations`               | Paginated latest locations                   |
| GET    | `/api/v1/locations/nearby`        | Vehicles within a radius, nearest first      |

The single-vehicle and list responses contain `vehicle_id`, `event_id`,
`latitude`, `longitude`, `speed_kmh`, `heading`, `recorded_at`, `received_at`,
optional `shipment_id`, `is_stale` and non-negative `age_seconds`.

```powershell
# One vehicle
curl.exe "http://127.0.0.1:8000/api/v1/locations/0b6f2c9a-1d2e-4f3a-8b7c-9d0e1f2a3b4c"

# One page, optionally filtered by freshness or vehicle
curl.exe "http://127.0.0.1:8000/api/v1/locations?page=1&page_size=20"
curl.exe "http://127.0.0.1:8000/api/v1/locations?is_stale=true"
curl.exe "http://127.0.0.1:8000/api/v1/locations?vehicle_id=0b6f2c9a-1d2e-4f3a-8b7c-9d0e1f2a3b4c"

# Nearby: Berlin centre, 5 km, nearest 20
curl.exe "http://127.0.0.1:8000/api/v1/locations/nearby?latitude=52.52&longitude=13.405&radius=5&unit=km&limit=20"
```

List responses use `items`, `page`, `page_size`, `total` and `total_pages`.
`page` defaults to `1`; `page_size` defaults to `20` and is capped by
`REDIS_LOCATION_MAX_PAGE_SIZE` (default `100`, hard API maximum `500`). Ordering
is deterministic for a fixed dataset: members come from the existing
`{prefix}:live:geo` sorted set in rank order (geohash score, then member). The
unfiltered path reads only the requested page of hashes, in one pipeline, and
uses `ZCARD` for `total`. Filtering by `is_stale` streams the whole GEO index in
bounded batches because freshness lives in the hash; that path is `O(N)` and
its `total` is the exact number of matches at scan time.

The projection is live, not a snapshot. A GPS update can move a vehicle in the
GEO rank or change the index size between page requests, so a client paginating
slowly may see an item on two adjacent pages or miss one. `total` is an
instantaneous count. Clients that need a stable view should request a larger
page or retry the sequence.

### Nearby search

| Parameter       | Required | Default | Rules                                              |
| --------------- | -------- | ------- | -------------------------------------------------- |
| `latitude`      | yes      | —       | `-90` … `90`                                       |
| `longitude`     | yes      | —       | `-180` … `180`                                     |
| `radius`        | yes      | —       | Positive, in `unit`; at most `REDIS_LOCATION_NEARBY_MAX_RADIUS_KM` (default `100` km) |
| `unit`          | no       | `km`    | `m`, `km` or `mi`                                  |
| `limit`         | no       | `50`    | `1` … `REDIS_LOCATION_NEARBY_MAX_LIMIT` (default `500`) |
| `exclude_stale` | no       | `false` | Drop stale vehicles from the candidate window      |

The endpoint uses `GEOSEARCH ... BYRADIUS ... ASC` and fetches all matching
hashes with one pipeline. The response echoes `center`, `radius` and `unit`, and
each item carries the same location/freshness fields plus `distance` in the
requested unit. No match is an empty `items` list with `200`. With
`exclude_stale=true`, up to three times `limit` nearest vehicles are read so
stale entries can be removed without scanning the entire GEO index; a very
stale-heavy area may therefore return fewer than `limit` items.

### Freshness and errors

`is_stale` is calculated from `recorded_at` and
`REDIS_LOCATION_STALE_AFTER_SECONDS` (default `60`); `age_seconds` is clamped
to zero for future timestamps. Redis key TTLs are never used for freshness and
stale vehicles are not removed from the GEO index.

| Condition                                  | Status | Error code            |
| ------------------------------------------ | ------ | --------------------- |
| No live location for the vehicle           | `404`  | `not_found`           |
| Invalid UUID, coordinates, unit or bounds  | `422`  | `validation_error`    |
| Redis unreachable, timing out or corrupt   | `503`  | `service_unavailable` |

Malformed hashes encountered during a page or nearby scan are logged and
skipped; a corrupt hash requested directly returns `503` instead of pretending
the vehicle is unknown. Redis is required for these reads: if it is unavailable
the API answers `503` with the standard envelope and no Redis internals. The
live projection is rebuildable and is not the source of truth; history,
geofencing, alerts and ETA features are later milestones.

## Run the application

```powershell
uvicorn app.main:app --reload
```

Or with uv:

```powershell
uv run uvicorn app.main:app --reload
```

The API listens on <http://127.0.0.1:8000> by default.

| Endpoint            | Purpose                                                   |
| ------------------- | --------------------------------------------------------- |
| `GET /`             | Basic API information                                     |
| `GET /health/live`  | Liveness probe; never touches PostgreSQL                  |
| `GET /health/ready` | Readiness probe; returns 200 or 503 based on `SELECT 1`   |
| `GET /docs`         | Swagger UI (development only)                             |
| `GET /redoc`        | ReDoc (development only)                                  |

A `503 Service Unavailable` readiness response means the API process is alive
but cannot reach PostgreSQL. Database errors are not exposed in the response.

## Test, lint and type-check

Unit tests do not require PostgreSQL, Kafka or Redis. The suite sets
`KAFKA_ENABLED=false` and `REDIS_ENABLED=false` (see `tests/conftest.py`), and
the default selection excludes the `integration`, `kafka`, `redis` and
`kafka_redis` markers:

```powershell
pytest
```

The GPS contract, GPS endpoint wiring, GPS publisher, producer lifecycle,
consumer processing, consumer transport, Redis live-location, live-location
read APIs, consumer→Redis handler, worker lifecycle and simulator tests are
unit tests and need no infrastructure:

```powershell
pytest tests/test_gps_schema.py tests/test_gps_ingestion_api.py tests/test_kafka_publisher.py tests/test_gps_consumer_service.py tests/test_gps_live_location_handler.py tests/test_gps_worker_redis.py tests/test_kafka_consumer_infra.py tests/test_live_location.py tests/test_live_location_api.py tests/test_redis_infrastructure.py tests/test_lifespan_redis.py tests/test_simulator.py
```

They cover valid and invalid events, coordinate and heading boundaries, naive
timestamps, GPS endpoint payload validation and OpenAPI wiring, producer
configuration (`acks=all`, idempotence, JSON payload, `vehicle_id` key, topic
creation), publish ordering and failure mapping, consumer JSON/schema
validation, in-memory deduplication (including not marking failures), bounded
retries, dead-letter envelopes, manual offset commits and commit ordering,
graceful shutdown, applied/duplicate/stale Redis outcomes, Redis connection,
timeout and OOM failures becoming retryable errors, Redis failure leaving the
offset uncommitted, worker Redis initialization/closure, Redis key namespacing,
hash serialisation, atomic updates, duplicate and out-of-order handling, GEO
lookups, staleness, Redis error translation, the Redis lifespan, deterministic
event generation, seed reproducibility, route interpolation, unique event IDs,
HTTP payloads, HTTP publisher success/failure reporting and simulator shutdown.
The live-location unit tests use `fakeredis` (including Lua) and never need a
server.

Integration tests require the Docker PostgreSQL service with migrations
applied, and are marked `integration`; they fail (not skip) if PostgreSQL is
unreachable. They cover REST CRUD, pagination and filtering, duplicate records,
shipment transitions, valid and invalid assignments, inactive resources,
concurrent assignment attempts, assignment history, double-ending, GPS
ingestion business rules and transaction rollback:

```powershell
docker compose up -d postgres
alembic upgrade head
pytest -m integration
```

Kafka broker tests are marked `kafka` (excluded from the default suite). They
need the Kafka broker; consumer tests do not touch PostgreSQL. They cover GPS
publication semantics against PostgreSQL, real-broker round trips (JSON payload,
`vehicle_id` key, explicit topic creation), the producer→consumer path, key and
content preservation, offset advancement, duplicate handling, malformed-message
quarantine and the no-commit path when DLQ publication fails. They use unique
per-test topics and consumer groups and clean them up:

```powershell
docker compose up -d postgres kafka
alembic upgrade head
pytest -m kafka
```

Redis integration tests are marked `redis` (also excluded from the default
suite). They need the Compose Redis service and cover connection/ping,
write/read, GEO radius lookup, atomic updates, duplicate and out-of-order
events, concurrent delivery convergence, repeated updates that must leave the
hash and GEO index consistent, rank pagination, stale filtering, empty results
and read-API smoke tests against real Redis. Every test uses an isolated key
namespace and deletes its keys:

```powershell
docker compose up -d redis
pytest -m redis
```

End-to-end consumer→Redis tests are marked `kafka_redis` and need **both**
services. They publish through the existing producer, run the real worker
components, verify the Redis hash and GEO index, cover newer/older/duplicate
delivery, and prove that a Redis outage leaves the Kafka offset uncommitted
until the event is safely dead-lettered. Each test uses an isolated topic, DLQ,
consumer group and key namespace:

```powershell
docker compose up -d kafka redis
pytest -m kafka_redis
```

For safe migration verification, use a dedicated test database rather than one
containing data you need to keep:

```powershell
docker compose exec -T postgres psql -U fleetpulse -d postgres `
  -c "CREATE DATABASE fleetpulse_test;"

$env:DATABASE_URL = "postgresql+asyncpg://fleetpulse:fleetpulse@localhost:55432/fleetpulse_test"
alembic upgrade head
pytest -m integration
alembic downgrade -1
alembic upgrade head
```

Adjust the port and credentials to match your `.env`. Downgrade is destructive;
only run it against an isolated test database.

Linting, formatting and typing:

```powershell
ruff check .
ruff format --check .
mypy app simulator tests
```

Apply formatting automatically:

```powershell
ruff format .
ruff check --fix .
```

## Troubleshooting

**Docker daemon is not running**

Start Docker Desktop and wait until `docker info` succeeds before running
`docker compose up`.

**Port 5432 is already in use**

A local PostgreSQL installation or another container may own the port. Pick a
free host port and update **both** values in `.env`, then recreate the container:

```dotenv
POSTGRES_PORT=55432
DATABASE_URL=postgresql+asyncpg://fleetpulse:fleetpulse@localhost:55432/fleetpulse
```

```powershell
docker compose up -d postgres
```

**Readiness returns 503**

```powershell
docker compose ps
docker compose logs postgres
```

Confirm the container is `healthy`, the port in `DATABASE_URL` matches
`POSTGRES_PORT`, and the credentials match the `POSTGRES_*` variables. Restart
the API after changing `.env`.

**`password authentication failed`**

The database volume may have been initialized with an older password. For local
development only, reset it with `docker compose down -v` followed by
`docker compose up -d postgres` (this deletes local data).

**Alembic cannot connect**

Run `alembic current` from the repository root so `.env` is found, and verify
`DATABASE_URL` uses the `postgresql+asyncpg://` scheme.

**GPS ingestion returns 503 (`service_unavailable`)**

The event passed validation but could not be delivered to Kafka (or
`KAFKA_ENABLED=false`). Check the broker:

```powershell
docker compose ps kafka
docker compose logs kafka
```

Confirm the container is `healthy` and `KAFKA_BOOTSTRAP_SERVERS` matches the
host port in `KAFKA_PORT`. The producer is created during startup, so restart
the API after changing `.env`. If startup fails with "Kafka is required but the
GPS producer failed to start", the broker was unreachable when the API booted.

**The GPS worker exits immediately or logs fetch failures**

The worker requires Kafka **and** Redis even when the API runs with
`KAFKA_ENABLED=false`/`REDIS_ENABLED=false`. Start both services and verify
connectivity:

```powershell
docker compose ps kafka redis
python -m app.workers.gps_worker --log-level DEBUG
```

Confirm `KAFKA_BOOTSTRAP_SERVERS`, `KAFKA_CONSUMER_GROUP_ID`,
`KAFKA_GPS_TOPIC` and `REDIS_URL` match the running services. If startup fails
with "Redis is unavailable", the worker exits with code `1` before fetching.
If the worker reports that a message could not be dead-lettered, check
`KAFKA_GPS_DLQ_TOPIC`; the original offset is intentionally left uncommitted
until the DLQ acknowledges it.

**The API fails at startup with "Redis is required"**

`REDIS_ENABLED=true` (the default) pings Redis during the FastAPI lifespan.
Check the service and URL:

```powershell
docker compose ps redis
docker compose logs redis
```

Confirm the container is `healthy` and `REDIS_URL` matches `REDIS_PORT`. Unit
tests set `REDIS_ENABLED=false`; do not disable Redis to work around a real
connectivity problem in development. Redis errors never include the URL or
credentials.

## Notes

- `.env` is ignored by Git; `.env.example` is committed as a template.
- Do not commit credentials or other secrets. Supply them through environment
  variables only.
- The database engine and Redis client are each created once per application
  process and disposed during FastAPI shutdown. The standalone GPS worker owns
  a separate Redis client and closes it on shutdown.
- Authentication, durable GPS persistence, location history, geofencing,
  alerts, ETA and Kubernetes remain out of scope. Milestones 5A–5C.2 provide
  the GPS event contract, a simulator, an HTTP ingestion endpoint, a Kafka
  producer and a standalone consumer worker: events are validated, published
  to `gps.events` and consumed at-least-once, but no GPS rows are stored, so
  `persisted` is always `false` in ingestion responses. The worker's
  deduplication is in-memory only.
- Milestone 6.1 adds the Redis client, live-location repository and service;
  Milestone 6.2 connects the Kafka consumer to that service; Milestone 6.3
  exposes single, paginated and nearby read endpoints. Redis holds a
  rebuildable projection and is not the source of truth. List pagination is
  live, not a snapshot, and `is_stale` filtering scans the GEO index.
- API tests create uniquely named records and do not truncate existing data;
  use a dedicated test database when running destructive migration checks.
