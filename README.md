# FleetPulse

FleetPulse is a Real-Time Fleet and Delivery Intelligence Platform. The
repository currently contains four milestones:

- **Milestone 1** — modular FastAPI application, Pydantic Settings, structured
  logging, health probes and a tested toolchain.
- **Milestone 2** — async PostgreSQL connectivity with SQLAlchemy 2.x, asyncpg,
  Alembic migrations and a Docker Compose development database.
- **Milestone 3** — core fleet domain models for vehicles, drivers, shipments
  and historical vehicle assignments.
- **Milestone 4** — versioned REST APIs, validation schemas, repositories,
  service-layer business rules, explicit shipment transitions and assignment
  concurrency protection.

Authentication, message brokers, caches, GPS tracking and Kubernetes manifests
remain intentionally out of scope.

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
|   |   |-- health.py
|   |   |-- shipment.py
|   |   |-- system.py
|   |   `-- vehicle.py
|   `-- services/
|       |-- assignment.py        # Assignment business rules
|       |-- driver.py
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
|   |-- test_health.py           # Unit tests (no PostgreSQL required)
|   |-- test_integrity_errors.py # Constraint translation tests
|   |-- test_models.py           # Model metadata tests (no PostgreSQL required)
|   |-- test_schemas.py          # Request/response validation tests
|   |-- test_shipment_transitions.py
|   `-- integration/
|       |-- test_api.py          # REST API tests against real PostgreSQL
|       |-- test_assignment_concurrency.py
|       |-- test_database.py     # Real PostgreSQL integration tests
|       `-- test_models.py       # Constraints and relationship tests
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

Integration tests require the Docker PostgreSQL service with migrations
applied, and are marked `integration`; they fail (not skip) if PostgreSQL is
unreachable. They cover REST CRUD, pagination and filtering, duplicate records,
shipment transitions, valid and invalid assignments, inactive resources,
concurrent assignment attempts, assignment history, double-ending and
transaction rollback:

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
mypy app tests
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
- Authentication, Kafka, Redis, GPS tracking and Kubernetes remain out of
  scope for this milestone.
- API tests create uniquely named records and do not truncate existing data;
  use a dedicated test database when running destructive migration checks.
