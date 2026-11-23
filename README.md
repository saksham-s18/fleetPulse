# FleetPulse

FleetPulse is a Real-Time Fleet and Delivery Intelligence Platform. The
repository currently contains six milestones:

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
  validates GPS events against the fleet domain and temporarily accepts them
  over HTTP (no persistence yet).

Authentication, message brokers, caches, durable GPS persistence, Kafka
ingestion and Kubernetes manifests remain intentionally out of scope.

## Technology stack

| Concern            | Tool                                    |
| ------------------ | --------------------------------------- |
| Language           | Python 3.12                             |
| Web framework      | FastAPI                                 |
| Validation/models  | Pydantic v2 + Pydantic Settings         |
| Database           | PostgreSQL 16 (Docker Compose locally)  |
| ORM / database     | SQLAlchemy 2.x async API + asyncpg      |
| Migrations         | Alembic                                 |
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
|   |       |-- locations.py     # POST /api/v1/locations (GPS ingestion)
|   |       |-- root.py          # GET /
|   |       |-- shipments.py     # Shipment endpoints and transitions
|   |       `-- vehicles.py      # Vehicle endpoints
|   |-- core/
|   |   |-- config.py            # Pydantic Settings (app + database)
|   |   |-- exceptions.py        # Domain errors mapped to HTTP responses
|   |   `-- logging.py           # Logging setup
|   |-- infrastructure/
|   |   |-- __init__.py
|   |   `-- database.py          # Async engine, session factory, Base, probes
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
|   |   |-- shipment.py
|   |   `-- vehicle.py
|   |-- schemas/
|   |   |-- assignment.py
|   |   |-- common.py            # Location, request base and Page[T]
|   |   |-- driver.py
|   |   |-- errors.py
|   |   |-- gps.py               # Broker-independent GpsEvent contract
|   |   |-- health.py
|   |   |-- shipment.py
|   |   |-- system.py
|   |   `-- vehicle.py
|   `-- services/
|       |-- assignment.py        # Assignment business rules
|       |-- driver.py
|       |-- gps.py               # GPS ingestion business rules (no writes yet)
|       |-- pagination.py
|       |-- shipment.py
|       |-- shipment_transitions.py
|       |-- transactions.py      # Commit/rollback and integrity translation
|       `-- vehicle.py
|-- alembic/
|   |-- env.py                   # Async migration environment
|   |-- script.py.mako
|   `-- versions/                # Migration revisions (baseline included)
|-- tests/
|   |-- test_api_errors.py       # Error envelope and OpenAPI tests
|   |-- test_database.py         # Unit tests (no PostgreSQL required)
|   |-- test_gps_ingestion_api.py # GPS endpoint wiring (no PostgreSQL)
|   |-- test_gps_schema.py       # GPS contract validation tests
|   |-- test_health.py           # Unit tests (no PostgreSQL required)
|   |-- test_integrity_errors.py # Constraint translation tests
|   |-- test_models.py           # Model metadata tests (no PostgreSQL required)
|   |-- test_schemas.py          # Request/response validation tests
|   |-- test_shipment_transitions.py
|   |-- test_simulator.py        # Movement, transport and shutdown tests
|   `-- integration/
|       |-- test_api.py          # REST API tests against real PostgreSQL
|       |-- test_assignment_concurrency.py
|       |-- test_database.py     # Real PostgreSQL integration tests
|       |-- test_gps_ingestion.py # GPS ingestion rules against PostgreSQL
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
file. Environment variables take precedence. `DATABASE_URL` is stored as a
`SecretStr`, so its value is masked in logs and representations.

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

OpenAPI documentation (`/docs`, `/redoc`, `/openapi.json`) is enabled only when
`APP_ENV=development`.

Create your local environment file:

```powershell
Copy-Item .env.example .env
```

The example contains local-development credentials only. Never commit `.env`
or real secrets; it is already excluded by `.gitignore`.

## Local PostgreSQL with Docker Compose

Start PostgreSQL:

```powershell
docker compose up -d postgres
docker compose ps
```

Wait until the service reports `healthy` (the compose health check uses
`pg_isready`). Data is stored in the named volume
`fleetpulse_fleetpulse_postgres_data`.

Follow logs or manage the service:

```powershell
docker compose logs -f postgres
docker compose stop postgres      # stop, keep container and data
docker compose start postgres     # start again
docker compose down               # remove container and network, keep data
docker compose down -v            # remove container AND database volume (destructive)
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

Known limitations as of Milestone 5B:

- The HTTP ingestion endpoint validates events but does not persist them. A
  `202` response is an acceptance acknowledgement, not a durability guarantee
  (see "GPS ingestion" below).
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
| POST   | `/api/v1/locations`                     | Accept a GPS event (temporary path)      |

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

## GPS ingestion (temporary acceptance path)

`POST /api/v1/locations` accepts the `GpsEvent` contract described above. The
route handler only parses the payload with the existing `GpsEvent` schema;
all business rules live in `app/services/gps.py` (`GpsIngestService`).

| Condition                                                     | Status | Error code         |
| ------------------------------------------------------------- | ------ | ------------------ |
| Valid event for an active vehicle                              | `202`  | —                  |
| Unknown `vehicle_id`                                          | `404`  | `not_found`        |
| Vehicle exists but is not `active`                            | `409`  | `conflict`         |
| Unknown `shipment_id`                                         | `404`  | `not_found`        |
| Shipment is not on the vehicle's current active assignment     | `409`  | `conflict`         |
| Payload fails `GpsEvent` validation                           | `422`  | `validation_error` |

Behaviour:

- Events without a `shipment_id` are accepted for any active vehicle.
- With a `shipment_id`, the shipment must exist and sit on the vehicle's
  **current** active assignment (`ended_at IS NULL`); events referencing a
  past assignment are rejected.
- A successful response is `202 Accepted`:

```json
{
  "event_id": "6f1c2b1e-9f7a-4b1a-9c2d-3a4b5c6d7e8f",
  "vehicle_id": "0b6f2c9a-1d2e-4f3a-8b7c-9d0e1f2a3b4c",
  "shipment_id": null,
  "status": "accepted",
  "persisted": false
}
```

**This path is deliberately non-durable.** No GPS rows, Kafka topics, Redis
queues or background workers exist in this milestone: `202` means only that
the event passed validation and was accepted in-process. The service keeps
validation and acceptance separate so the next milestone can publish to Kafka
at the acceptance step without rewriting these rules or the endpoint.

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

Unit tests do not require PostgreSQL:

```powershell
pytest
```

The GPS contract, GPS endpoint wiring and simulator tests are unit tests and
need neither PostgreSQL nor a running API:

```powershell
pytest tests/test_gps_schema.py tests/test_gps_ingestion_api.py tests/test_simulator.py
```

They cover valid and invalid events, coordinate and heading boundaries, naive
timestamps, GPS endpoint payload validation and OpenAPI wiring, deterministic
event generation, seed reproducibility, route interpolation, unique event IDs,
HTTP payloads, HTTP publisher success/failure reporting and graceful shutdown.

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

## Notes

- `.env` is ignored by Git; `.env.example` is committed as a template.
- Do not commit credentials or other secrets. Supply them through environment
  variables only.
- The database engine is created once per application process and disposed
  during FastAPI shutdown.
- Authentication, Kafka, Redis, durable GPS persistence and Kubernetes remain
  out of scope. Milestones 5A and 5B provide the GPS event contract, a
  simulator and an HTTP acceptance endpoint; events are validated but no GPS
  rows are stored, and `persisted` is always `false` in ingestion responses.
- API tests create uniquely named records and do not truncate existing data;
  use a dedicated test database when running destructive migration checks.
