# HR Insights

HR Insights is an ETL pipelin for Employee and Timehseet data. It ingests raw employee and timesheet data into bronze/silver layers, builds gold-layer analytics, and exposes a JSON API over the silver and gold models.

## Overview

- **ETL**: Luigi-based pipeline that loads source data into bronze and silver schemas and computes gold-layer KPIs (headcount, turnover, attendance, organization metrics).
- **API**: FastAPI application that provides CRUD for employees, read-only access to timesheets, and read-only analytics endpoints. All API responses use a consistent envelope: `success`, `data`, `message`, and `error` (on failure). Authentication is JWT-based; role-based authorization restricts employee write operations to the admin role.

## Prerequisites

- Python 3.x
- PostgreSQL (for the application database)
- Environment or `.env` for `DATABASE_URL` and optional API/JWT settings

## Setup

1. **Install dependencies**

   From the project root:

   ```bash
   pip install -e .
   ```

   Or with uv: `uv sync`

2. **Configure database**

   Set `DATABASE_URL` if needed (default: `postgresql://hr_insights:hr_insights@localhost:5432/hr_insights`). Create the database and schemas/tables:

   ```bash
   python -m src.app.database
   ```

   Or use Alembic:

   ```bash
   alembic upgrade head
   ```

3. **Run the ETL pipeline** (to populate silver and gold data)

   ```bash
   python -m src.etl.run --module src.etl.gold.tasks LoadAllGold --local-scheduler --all-months True
   ```

   For local CSVs (in `data/raw`): add `--source local`. The pipeline is rerun-safe: new or changed files trigger reprocessing.

4. **Start the API**

   ```bash
   uvicorn src.app.main:app --reload --port 5173
   ```

   Or:

   ```bash
   python -m src.app.main
   ```

   The API listens on the host/port defined in `API_HOST` and `API_PORT` (defaults: `0.0.0.0`, `5173`).

## API Documentation

Interactive OpenAPI docs are available at `/docs` (Swagger UI) and `/redoc` (ReDoc). The OpenAPI JSON is at `/openapi.json`.

### Authentication

All endpoints except `/health` and `POST /token` require a valid JWT in the `Authorization` header:

```
Authorization: Bearer <access_token>
```

**Obtain a token**

- **Endpoint**: `POST /token`
- **Body**: `{"username": "<username>", "password": "<password>"}`
- **Response**: `{"success": true, "data": {"access_token": "<jwt>", "token_type": "bearer"}, "message": "..."}`

Demo users (in-memory): `admin` and `viewer`. Both use the same default password (see `src.app.core.user_store`). Admin can create, update, and delete employees; viewer can only read.

### Authorization

- **admin**: Full CRUD on employees; read access to timesheets and analytics.
- **viewer**: Read-only access to employees, timesheets, and analytics. Write operations on employees return 403.

### Response format

Every response is a JSON object with:

- `success` (boolean): Whether the request succeeded.
- `message` (string): Human-readable summary.
- `data` (optional): Response payload (list or object); omitted when empty or not applicable.
- `error` (optional): Present on failure; error detail string.

### Endpoints summary

| Area | Method | Path | Description |
|------|--------|------|-------------|
| Auth | POST | `/token` | Exchange username/password for a JWT. |
| Health | GET | `/health` | Service health; no auth. |
| Employees | POST | `/employees` | Create employee (admin). |
| Employees | GET | `/employees` | List employees (paginated). |
| Employees | GET | `/employees/{employee_id}` | Get one employee by `client_employee_id`. |
| Employees | PATCH | `/employees/{employee_id}` | Update employee (admin). |
| Employees | DELETE | `/employees/{employee_id}` | Delete employee (admin). |
| Timesheets | GET | `/timesheets` | List timesheets (paginated). |
| Timesheets | GET | `/timesheets/employee/{employee_id}` | Timesheets for one employee. |
| Timesheets | GET | `/timesheets/filter?start_date=&end_date=&employee_id=` | Timesheets in date range, optional employee filter. |
| Analytics | GET | `/analytics/headcount` | Headcount trend (optional year, month, limit). |
| Analytics | GET | `/analytics/departments` | Department metrics (optional filters, limit). |
| Analytics | GET | `/analytics/employees/{employee_id}/attendance` | Attendance for one employee. |
| Analytics | GET | `/analytics/employees/attendance` | Attendance for all employees (paginated). |
| Analytics | GET | `/analytics/organization` | Organization-wide metrics. |
| Analytics | GET | `/analytics/timesheets/daily` | Daily timesheet summary (optional employee, date range). |

Path and query parameters are described in the OpenAPI schema at `/docs` or `/openapi.json`.

## Project structure (API)

The API lives under `src/app/`:

- **main.py**: FastAPI app, exception handlers, router registration, `/health`.
- **api/**: Route handlers (auth, employees, timesheets, analytics).
- **core/**: Config, logging, security (JWT, password, `get_current_user`, `require_roles`), roles, user store, exceptions.
- **schemas/**: Pydantic request/response models (auth, employee, timesheet, analytics, response envelope).
- **services/**: Business logic for employees, timesheets, and analytics (no HTTP).
- **database.py**: Engine, session factory, `get_db` dependency, `init_db` for schemas/tables.

ETL and data models live under `src/etl/`, `src/db/`, and related modules.

## Environment variables

| Variable | Default | Used by |
|----------|---------|--------|
| `DATABASE_URL` | `postgresql://hr_insights:hr_insights@localhost:5432/hr_insights` | DB, ETL |
| `HR_INSIGHTS_MINIO_ENDPOINT` | `localhost:9000` | ETL (MinIO) |
| `HR_INSIGHTS_MINIO_ACCESS_KEY` | `minioadmin` | ETL |
| `HR_INSIGHTS_MINIO_SECRET_KEY` | `minioadmin` | ETL |
| `HR_INSIGHTS_MINIO_SECURE` | `false` | ETL |
| `HR_INSIGHTS_MINIO_BUCKET` | `hr-insights` | ETL |
| `HR_INSIGHTS_RAW_DATA_DIR` | `data/raw` | ETL |
| `HR_INSIGHTS_MANIFESTS_DIR` | `logs/manifests` | ETL |
| `HR_INSIGHTS_ETL_BATCH_SIZE` | `1000` | ETL (bronze load) |
| `HR_INSIGHTS_ETL_DEFAULT_YEAR` | `0` (use current) | ETL (gold when year not set) |
| `HR_INSIGHTS_ETL_DEFAULT_MONTH` | `0` (use current) | ETL (gold when month not set) |
| `API_HOST` | `0.0.0.0` | API |
| `API_PORT` | `5173` | API |
| `API_DEBUG` | `false` | API |
| `API_ENV_NAME` | `development` | API |
| `API_RELOAD` | `true` | API |

All of the above can be set in a `.env` file in the project root. Database/JWT: `echo`, `secret_key`, `algorithm`, `access_token_expire_minutes` via pydantic-settings in `src.app.database`.

## Analytics SQL

Pre-built KPI queries for the gold layer are in `src/etl/gold/metrics/kpis.sql`.

## Dashboard

After gold data is populated, generate visualizations:

```bash
python dashboard/visualize.py
```

Output HTML files are written to `dashboard/output/`.
