# FleetPulse

FleetPulse is a Real-Time Fleet and Delivery Intelligence Platform. This
repository currently contains the **foundation milestone**: a modular FastAPI
backend with environment-based configuration, structured logging, health probes
and a tested project toolchain.

No business features, database models, message brokers, caches or container
manifests are included yet.

## Technology stack

| Concern            | Tool                          |
| ------------------ | ----------------------------- |
| Language           | Python 3.12                   |
| Web framework      | FastAPI                       |
| Validation/models  | Pydantic v2 + Pydantic Settings |
| ASGI server        | Uvicorn                       |
| Linting/formatting | Ruff                          |
| Static typing      | Mypy (strict)                 |
| Testing            | Pytest + HTTPX TestClient     |
| Project config     | `pyproject.toml`              |

## Prerequisites

- Windows 10/11 with PowerShell 7+ (Windows PowerShell 5.1 also works)
- Python 3.12 installed and available as `py -3.12` or `python`
- Git

Optional: [uv](https://docs.astral.sh/uv/) for faster environment and dependency
management.

## Project layout

```text
fleetpulse/
|-- app/
|   |-- __init__.py
|   |-- main.py                  # FastAPI application factory and entry point
|   |-- api/
|   |   |-- __init__.py
|   |   `-- routes/
|   |       |-- __init__.py
|   |       |-- health.py        # GET /health/live, GET /health/ready
|   |       `-- root.py          # GET /
|   |-- core/
|   |   |-- __init__.py
|   |   |-- config.py            # Pydantic Settings
|   |   `-- logging.py           # Logging setup
|   |-- schemas/
|   |   |-- __init__.py
|   |   |-- health.py
|   |   `-- system.py
|   |-- services/                # Reserved for business logic
|   |-- repositories/            # Reserved for data access
|   `-- infrastructure/          # Reserved for adapters (brokers, caches, clients)
|-- tests/
|   |-- __init__.py
|   `-- test_health.py
|-- .env.example
|-- .gitignore
|-- pyproject.toml
`-- README.md
```

## Configuration

Configuration is read from environment variables, then from an optional `.env`
file. Environment variables take precedence. Defaults are safe for local
development and no secrets are required at this stage.

| Variable      | Default         | Allowed values                                      |
| ------------- | --------------- | --------------------------------------------------- |
| `APP_NAME`    | `FleetPulse API`| Any non-empty string                                |
| `APP_VERSION` | `0.1.0`         | Any non-empty string                                |
| `APP_ENV`     | `development`   | `development`, `staging`, `production`, `test`      |
| `LOG_LEVEL`   | `INFO`          | `DEBUG`, `INFO`, `WARNING`, `ERROR`, `CRITICAL`     |

OpenAPI documentation (`/docs`, `/redoc`, `/openapi.json`) is enabled only when
`APP_ENV=development`.

Create your local environment file:

```powershell
Copy-Item .env.example .env
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
```

### Setup with uv (optional)

```powershell
uv venv --python 3.12
uv pip install -e ".[dev]"
```

## Run the application

```powershell
uvicorn app.main:app --reload
```

Or with uv:

```powershell
uv run uvicorn app.main:app --reload
```

The API listens on <http://127.0.0.1:8000> by default.

| Endpoint       | Purpose                                      |
| -------------- | -------------------------------------------- |
| `GET /`        | Basic API information                        |
| `GET /health/live`  | Liveness probe (process is running)     |
| `GET /health/ready` | Readiness probe (ready to serve traffic)|
| `GET /docs`    | Swagger UI (development only)                |
| `GET /redoc`   | ReDoc (development only)                     |

## Test, lint and type-check

```powershell
pytest
ruff check .
ruff format --check .
mypy app tests
```

Apply formatting automatically:

```powershell
ruff format .
ruff check --fix .
```

## Notes

- `.env` is ignored by Git; `.env.example` is committed as a template.
- Do not commit credentials or other secrets. Add future secrets through
  environment variables only.
