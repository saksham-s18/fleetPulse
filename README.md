# FleetPulse

FleetPulse is a Real-Time Fleet and Delivery Intelligence Platform. The
repository currently contains the first two foundation milestones:

- **Milestone 1** — modular FastAPI application, Pydantic Settings, structured
  logging, health probes and a tested toolchain.
- **Milestone 2** — async PostgreSQL connectivity with SQLAlchemy 2.x, asyncpg,
  Alembic migrations and a Docker Compose development database.

No business features, ORM business models, message brokers, caches or
Kubernetes manifests are included yet.

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
|   |-- main.py                  # App factory, lifespan and engine disposal
|   |-- api/
|   |   `-- routes/
|   |       |-- health.py        # GET /health/live, GET /health/ready
|   |       `-- root.py          # GET /
|   |-- core/
|   |   |-- config.py            # Pydantic Settings (app + database)
|   |   `-- logging.py           # Logging setup
|   |-- infrastructure/
|   |   |-- __init__.py
|   |   `-- database.py          # Async engine, session factory, Base, probes
|   |-- schemas/
|   |   |-- health.py
|   |   `-- system.py
|   |-- services/                # Reserved for business logic
|   `-- repositories/            # Reserved for data access
|-- alembic/
|   |-- env.py                   # Async migration environment
|   |-- script.py.mako
|   `-- versions/                # Migration revisions
|-- tests/
|   |-- test_database.py         # Unit tests (no PostgreSQL required)
|   |-- test_health.py           # Unit tests (no PostgreSQL required)
|   `-- integration/
|       `-- test_database.py     # Real PostgreSQL integration tests
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

There are no business tables or migration revisions yet; the baseline revision
should be created when the first model is added. Import new model modules in
`alembic/env.py` (or from a central models module) so autogeneration can see
their metadata.

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

Integration tests require the Docker PostgreSQL service and are marked
`integration`; they fail (not skip) if PostgreSQL is unreachable:

```powershell
docker compose up -d postgres
pytest -m integration
```

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
- Future work (business models, Kafka, Redis, Kubernetes) is intentionally out
  of scope for this milestone.
