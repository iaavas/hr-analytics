# HR Insights Project Documentation

This document is the implementation-focused reference for the HR Insights project. It explains the engineering choices behind the system, how to set up and run the ETL pipeline, how reports are generated, and how the database is structured across the bronze, silver, and gold layers.

## Table of Contents

1. [Engineering Decisions](#engineering-decisions)
2. [Setup Instructions](#setup-instructions)
3. [Usage Guide](#usage-guide)
4. [Schema Documentation](#schema-documentation)

## Engineering Decisions

### System Architecture

![System Architecture](images/System%20Architecture.png)

### Technology Choices

| Area | Choice | Why it was chosen | Trade-off |
| --- | --- | --- | --- |
| Language | Python 3.10+ | The project needs data processing, API development, and scripting in one stack. Python covers pandas, SQLAlchemy, Luigi, FastAPI, and Plotly without additional integration overhead. | Runtime performance is lower than compiled languages, but developer speed is higher for ETL work. |
| Orchestration | Luigi | The ETL flow is dependency-driven and naturally modeled as tasks with upstream requirements. Luigi also provides simple local scheduling and file-based completion markers. | Luigi is less feature-rich than Airflow/Prefect for monitoring and distributed scheduling. |
| Database | PostgreSQL | The system needs relational integrity for employees, departments, organizations, and timesheets. PostgreSQL also handles multiple schemas cleanly. | A relational model is stricter than a data lake layout, so ingestion still needs normalization work before analytics. |
| Raw storage | MinIO | Docker deployments need an S3-compatible source for raw CSV ingestion. MinIO keeps that workflow close to production-style object storage while still running locally. | It introduces one more service compared with a filesystem-only workflow. |
| ORM / migrations | SQLAlchemy 2.x + Alembic | SQLAlchemy is already used by both the API and ETL. Alembic provides repeatable schema evolution. | ORM models and migrations must stay aligned manually. |
| API | FastAPI | The project exposes curated silver/gold data through a typed JSON API with automatic OpenAPI documentation. | It adds another runtime surface area to maintain beyond the ETL pipeline. |
| Reporting | Plotly HTML dashboards | The analytics outputs need to be shareable without a separate BI tool. Static HTML reports are easy to archive and open locally. | HTML snapshots are not a multi-user analytics platform. |

### Architecture Decisions

#### 1. Medallion layering instead of one shared schema

![Medallion Architecture](images/Medallion%20Architecture.png)

The project separates data into three schemas:

- `bronze`: raw ingestion tables that preserve source structure and source file lineage.
- `silver`: cleaned and normalized operational tables used by the API and by downstream metrics.
- `gold`: denormalized analytical tables built for reporting and fast reads.

Why this matters:

- Raw files can be reprocessed without losing the original shape of the source data.
- Cleaning logic is isolated in one place instead of being repeated in reports and APIs.
- Gold tables stay focused on analytics and do not need to carry every raw column.

Trade-off:

- The same business entity appears in multiple layers, so storage and transformation complexity increase.

#### 2. Raw fidelity in bronze, strong cleanup in silver

Bronze models intentionally keep most fields as strings and store `source_file`. That reduces ingestion failure risk when source files contain malformed dates, inconsistent booleans, or missing numeric values. Cleanup is deferred to the silver transformation step where the project:

- trims strings and normalizes blanks,
- parses dates and datetimes,
- converts booleans such as active status and per-diem flags,
- calculates derived values such as tenure, worked minutes, scheduled minutes, lateness, overtime, and work date,
- drops duplicates before loading constrained silver tables.

Trade-off:

- Bronze is not immediately query-friendly, but it is much safer as a landing zone.

#### 3. Luigi markers and manifest hashing for rerun behavior

![ETL Flow](images/ETL.png)

The pipeline uses Luigi `LocalTarget` marker files under `logs/markers/` and manifest files under `logs/manifests/`.

The local-source discovery task hashes file contents. The MinIO discovery task stores object names plus etags. Those hashes flow into Luigi outputs, which means:

- rerunning the same inputs does not unnecessarily repeat completed work,
- changed files produce new manifests and therefore new downstream markers,
- the pipeline remains simple to run locally without an external scheduler database.

Trade-off:

- State is filesystem-based, so it is best suited to local and small-team workflows rather than large distributed orchestration.

#### 4. Quality gates before gold analytics

The project validates bronze and silver data before continuing:

- bronze checks ensure employee and timesheet rows have a usable `client_employee_id`,
- silver checks enforce uniqueness, employee-to-timesheet referential integrity, and sensible hour/date ranges.

This prevents the gold layer and dashboards from silently aggregating obviously broken data.

Trade-off:

- The pipeline fails fast on bad data instead of partially loading analytics, so operators need to fix source or transformation issues before reports are refreshed.

#### 5. Precomputed gold metrics instead of ad hoc dashboard queries

Gold tables such as `headcount_trend`, `employee_attendance_metrics`, and `department_monthly_metrics` are materialized ahead of time. This keeps dashboard generation and API reads straightforward because the expensive business logic is already resolved.

Trade-off:

- Gold tables must be regenerated whenever source data changes, but reads become much faster and easier to reason about.

## Setup Instructions

### Prerequisites

- Python 3.10 or newer
- PostgreSQL 15 or newer
- `pip` or `uv`
- Optional: Docker Desktop or Docker Engine with Compose support

### Local Development Setup

#### 1. Install dependencies

From the repository root:

```bash
uv sync
```

If you prefer `pip`:

```bash
pip install -e .
```

#### 2. Create environment configuration

Start from the provided example file:

```bash
cp .env.example .env
```

Minimum local setting:

```env
DATABASE_URL=postgresql://hr_insights:hr_insights@localhost:5432/hr_insights
```

Optional ETL-related settings in `.env`:

```env
HR_INSIGHTS_MINIO_ENDPOINT=localhost:9000
HR_INSIGHTS_MINIO_ACCESS_KEY=minioadmin
HR_INSIGHTS_MINIO_SECRET_KEY=minioadmin
HR_INSIGHTS_MINIO_SECURE=false
HR_INSIGHTS_MINIO_BUCKET=hr-insights
HR_INSIGHTS_RAW_DATA_DIR=data/raw
HR_INSIGHTS_MANIFESTS_DIR=logs/manifests
HR_INSIGHTS_ETL_BATCH_SIZE=1000
```

#### 3. Start PostgreSQL

Use either a locally installed PostgreSQL instance or the Compose service:

```bash
docker compose up -d postgres
```

#### 4. Create schemas and tables

You can initialize the database in either of these ways:

```bash
alembic upgrade head
```

or

```bash
python -m src.app.database
```

Both approaches create the `bronze`, `silver`, and `gold` schemas plus the mapped tables.

#### 5. Prepare input files

Place raw CSV files in `data/raw/`.

Current ETL behavior expects:

- filenames starting with `employee` to load into `bronze.employee_raw`
- filenames starting with `timesheet` to load into `bronze.timesheet_raw`
- pipe-delimited files (`|`) when read by the bronze loader

If a filename does not start with one of those prefixes, the bronze loader will skip it.

#### 6. Optional: start MinIO for object-storage ingestion

If you want the ETL to read from MinIO instead of local files:

```bash
docker compose up -d minio
```

Then upload the raw files:

```bash
python src/etl/upload_to_minio.py data/raw
```

### Full Docker Setup

If you want the entire stack in containers:

```bash
docker compose up --build
```

This starts:

- `postgres` for the application database,
- `minio` for raw file storage,
- `api` for migrations, ETL execution, dashboard generation, and the FastAPI service.

At startup, the `api` service runs:

1. `alembic upgrade head`
2. `python src/etl/upload_to_minio.py data/raw`
3. `python -m luigi --module src.etl.gold.tasks LoadAllGold --local-scheduler --all-months`
4. `python dashboard/visualize.py`
5. `uvicorn src.app.main:app --host 0.0.0.0 --port 5173`

Service URLs:

- API docs: `http://localhost:5173/docs`
- MinIO console: `http://localhost:9001`

To stop services:

```bash
docker compose down
```

To remove persisted volumes as well:

```bash
docker compose down -v
```

## Usage Guide

### Pipeline Execution Order

The end-to-end flow is:

1. discover files from local storage or MinIO,
2. extract raw CSV files into `data/raw/` when needed,
3. load bronze raw tables,
4. validate bronze,
5. transform and load silver tables,
6. validate silver,
7. build gold analytical tables,
8. write a QC report,
9. generate HTML dashboards.

### Recommended Commands

#### Run the full ETL, QC report, and dashboard generation from local files

```bash
python -m src.etl.run --module src.etl.quality.tasks RunDashboards --local-scheduler --source local --all-months True
```

Use this when your source CSV files are already under `data/raw/`.

#### Run the full ETL, QC report, and dashboard generation from MinIO

```bash
python -m src.etl.run --module src.etl.quality.tasks RunDashboards --local-scheduler --all-months True
```

`source` defaults to `minio`, so this command expects files to already be uploaded to the configured bucket.

#### Run only through the gold layer

```bash
python -m src.etl.run --module src.etl.gold.tasks LoadAllGold --local-scheduler --all-months True
```

Use this when you want analytical tables refreshed but do not need the QC report or dashboards.

#### Run the QC report without dashboard generation

```bash
python -m src.etl.run --module src.etl.quality.tasks RunQCReport --local-scheduler --all-months True
```

### Generated Outputs

After the pipeline runs, check these locations:

| Output | Location | Notes |
| --- | --- | --- |
| Luigi manifests | `logs/manifests/` | Tracks discovered source files and input hashes/etags |
| Luigi markers | `logs/markers/` | Marks completed bronze, silver, gold, QC, and dashboard tasks |
| QC reports | `logs/reports/` | Text reports such as `qc_report_YYYYMMDD_HHMMSS.txt` |
| Dashboards | `dashboard/output/` | Plotly HTML files for workforce, hours/overtime, attendance, and heatmap views |

The dashboard script currently writes:

- `dashboard/output/workforce_trend.html`
- `dashboard/output/work_hours_overtime.html`
- `dashboard/output/attendance_discipline.html`
- `dashboard/output/attendance_heatmap.html`

### Generate dashboards directly

If gold tables are already populated, you can regenerate dashboards without rerunning the full ETL:

```bash
python dashboard/visualize.py
```

The script reads `DATABASE_URL` from the environment unless `--db` is provided.

### Start the API

Once the database is populated, start the API locally:

```bash
uvicorn src.app.main:app --reload --port 5173
```

Available docs:

- Swagger UI: `http://localhost:5173/docs`
- ReDoc: `http://localhost:5173/redoc`

## Schema Documentation

### Database Model

![Database Model](images/Database%20Model.png)

### Schema Roles

| Schema | Purpose | Design style |
| --- | --- | --- |
| `bronze` | Raw landing zone for source files | Minimal transformation, preserves source fidelity |
| `silver` | Cleaned operational model | Typed, deduplicated, relational, API-friendly |
| `gold` | Reporting and KPI layer | Denormalized analytical marts |

### Core Entity Relationships

#### Bronze layer

Bronze is intentionally light on relationships.

- `bronze.employee_raw`: raw employee extracts plus `source_file`
- `bronze.timesheet_raw`: raw timesheet extracts plus `source_file`

These tables are staging tables, not the authoritative relational model.

#### Silver layer

Silver contains the main normalized relationships:

- `silver.organization` 1-to-many `silver.department`
- `silver.organization` 1-to-many `silver.employee`
- `silver.department` 1-to-many `silver.employee`
- `silver.employee` self-references `silver.employee.client_employee_id` through `manager_employee_id`
- `silver.employee` 1-to-many `silver.timesheet`
- `silver.department` is referenced by `silver.timesheet.department_id`

Key design details:

- `silver.employee.client_employee_id` is the primary employee business key.
- `silver.timesheet` enforces uniqueness on `(client_employee_id, punch_in_datetime, punch_out_datetime)`.
- `silver.timesheet` is indexed by employee and work date for downstream analytics.
- employees without matching timesheet references are protected by validation and loader filtering.

#### Gold layer

Gold tables are derived from silver and are intentionally optimized for reads rather than strict normalization:

| Table | Grain | Purpose |
| --- | --- | --- |
| `gold.employee_monthly_snapshot` | one row per employee per month | active status, tenure, job and department snapshot |
| `gold.timesheet_daily_summary` | one row per employee per day | shift counts, worked hours, lateness, early departure, overtime |
| `gold.employee_attendance_metrics` | one row per employee per month | attendance and hours KPIs |
| `gold.department_monthly_metrics` | one row per department per month | headcount, hires, terminations, turnover, attendance rates |
| `gold.headcount_trend` | one row per month | organization-level headcount and attrition trend |
| `gold.organization_metrics` | one row per month | company-wide aggregates |

Gold tables do not currently declare foreign keys back to silver. That is deliberate: they function as analytics marts and are rebuilt from silver as needed.

### Data Lineage Summary

The main lineage path is:

1. raw employee and timesheet CSV files land in local storage or MinIO,
2. bronze stores them with minimal coercion and file lineage,
3. silver normalizes organizations, departments, employees, and timesheets,
4. gold computes daily, monthly, department, employee, and organization metrics,
5. dashboards and API endpoints read from silver and gold.

### Practical Relationship Notes

- `organization_id`, `department_id`, and `client_employee_id` are the identifiers that connect most records across layers.
- manager relationships only exist in silver because bronze should not assume referential correctness.
- gold attendance metrics depend on valid `work_date`, shift durations, and the lateness/early-departure calculations produced in the silver transformation layer.
- if source employee data changes, gold headcount and tenure outputs will change even without timesheet changes because employee snapshots are recomputed from silver each run.
