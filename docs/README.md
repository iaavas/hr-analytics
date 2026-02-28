# HR Insights Documentation

## Table of Contents
- [Engineering Decisions](#engineering-decisions)
- [Setup Instructions](#setup-instructions)
- [Usage Guide](#usage-guide)
- [Schema Documentation](#schema-documentation)

---

## Engineering Decisions

### Technology Choices

| Component | Technology | Rationale |
|-----------|------------|-----------|
| **Language** | Python 3.10+ | Rich data processing ecosystem (pandas, sqlalchemy) |
| **ETL Framework** | Luigi | Robust task orchestration with dependency management, rerun-safety via markers |
| **API Framework** | FastAPI | High-performance async API with automatic OpenAPI docs |
| **Database** | PostgreSQL | ACID compliance, relational integrity for employee/timesheet data |
| **Object Storage** | MinIO | S3-compatible storage for raw CSV ingestion (Docker deployments) |
| **ORM** | SQLAlchemy 2.0 | Type-safe database access with Alembic migrations |
| **Visualization** | Plotly | Interactive HTML dashboards |
| **Authentication** | JWT (python-jose) | Stateless API authentication with role-based access |

### Architecture Decisions

**Medallion Architecture (Bronze/Silver/Gold)**
- **Bronze**: Raw ingestion layer - raw CSV data loaded with minimal transformation, preserving source structure
- **Silver**: Cleaned/normalized layer - deduplicated, type-converted, referentially intact data
- **Gold**: Analytics layer - pre-computed KPIs, aggregates, and metrics

**Trade-offs**
- Pre-computed gold metrics vs. on-the-fly calculation: Chose pre-computed for query performance at scale
- MinIO vs. local filesystem: MinIO enables containerized deployments with persistent raw data
- JWT vs. session auth: Stateless authentication scales better for API-first design

---

## Setup Instructions

### Prerequisites
- Python 3.10+
- PostgreSQL 15+
- (Optional) Docker & Docker Compose for containerized deployment

### Local Development Setup

1. **Clone and install dependencies**
   ```bash
   pip install -e .
   # or with uv: uv sync
   ```

2. **Configure environment**
   
   Create `.env` file (or use defaults):
   ```bash
   DATABASE_URL=postgresql://hr_insights:hr_insights@localhost:5432/hr_insights
   ```

3. **Initialize database**
   ```bash
   # Create schemas and tables
   python -m src.app.database
   
   # Or use Alembic migrations
   alembic upgrade head
   ```

4. **Add raw data**
   
   Place CSV files in `data/raw/`:
   - Employee data: `employees.csv`
   - Timesheet data: `timesheets.csv`

### Docker Deployment

1. **Create environment file**
   ```bash
   cp .env.example .env
   ```

2. **Build and run**
   ```bash
   docker compose up --build
   ```

3. **Access services**
   - API: http://localhost:5173/docs
   - MinIO Console: http://localhost:9001 (minioadmin/minioadmin)

---

## Usage Guide

### Running the ETL Pipeline

**Gold layer only (KPIs):**
```bash
python -m src.etl.run --module src.etl.gold.tasks LoadAllGold --local-scheduler --all-months True
```

**Full pipeline with QC report and dashboards (recommended):**
```bash
python -m src.etl.run --module src.etl.quality.tasks RunDashboards --local-scheduler --all-months True
```

**For local CSVs (bypassing MinIO):**
```bash
python -m src.etl.run --module src.etl.gold.tasks LoadAllGold --local-scheduler --source local
```

### Starting the API

```bash
uvicorn src.app.main:app --reload --port 5173
```

API docs available at http://localhost:5173/docs

### Authentication

1. **Get token**:
   ```bash
   curl -X POST http://localhost:5173/token \
     -H "Content-Type: application/json" \
     -d '{"username": "admin", "password": "password"}'
   ```

2. **Use token**:
   ```
   Authorization: Bearer <access_token>
   ```

**Demo users**: `admin` (full CRUD), `viewer` (read-only)

### Generating Dashboards

After gold data is populated:
```bash
python dashboard/visualize.py
```

Outputs: `dashboard/output/*.html`

---

## Schema Documentation

### Entity Relationship Diagram

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                                BRONZE LAYER                                  │
│                         (Raw Ingestion - source CSV)                        │
├─────────────────────────────────────────────────────────────────────────────┤
│  employee_raw                    timesheet_raw                              │
│  ────────────────               ─────────────────                           │
│  id (PK)                        id (PK)                                     │
│  client_employee_id             client_employee_id                         │
│  first_name                     department_id                               │
│  last_name                      punch_in_datetime                          │
│  ... (all raw fields)           punch_out_datetime                         │
│  source_file                    hours_worked                                │
│                                  source_file                                │
└─────────────────────────────────────────────────────────────────────────────┘
                                      │
                                      │ (ETL: clean, type-convert, deduplicate)
                                      ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                                SILVER LAYER                                  │
│                      (Normalized, referentially intact)                     │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│   organization              department              employee              │
│   ─────────────            ───────────            ─────────────           │
│   organization_id (PK)     department_id (PK)     client_employee_id (PK) │
│   organization_name        department_name        first_name              │
│                            organization_id (FK)    last_name               │
│                                                    job_title                │
│                                                    organization_id (FK)    │
│                                                    department_id (FK)      │
│                                                    manager_employee_id (FK)│
│                                                    hire_date               │
│                                                    term_date               │
│                                                    is_active               │
│                                                    is_per_diem             │
│                                                    ─────────────           │
│                                                    timesheets (1:M)        │
│                                                                             │
│   timesheet                                                                │
│   ─────────                                                                │
│   id (PK)                                                                 │
│   client_employee_id (FK)  ──────► employee.client_employee_id           │
│   department_id (FK)       ──────► department.department_id               │
│   punch_apply_date                                                           │
│   punch_in_datetime                                                          │
│   punch_out_datetime                                                         │
│   hours_worked                                                                │
│   work_date                                                                   │
│   pay_code                                                                    │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
                                      │
                                      │ (ETL: aggregate, compute metrics)
                                      ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                                 GOLD LAYER                                   │
│                        (Pre-computed KPIs and metrics)                      │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│   employee_monthly_snapshot                                                │
│   ───────────────────────────                                               │
│   id (PK)                                                                  │
│   client_employee_id                                                        │
│   year, month                                                               │
│   is_active, hire_date, term_date                                          │
│   department_id, department_name, job_title                                │
│   tenure_days                                                               │
│                                                                             │
│   timesheet_daily_summary                                                   │
│   ──────────────────────                                                    │
│   id (PK)                                                                  │
│   client_employee_id, work_date                                             │
│   total_hours_worked, total_shifts                                          │
│   late_minutes_total, early_minutes_total, overtime_minutes_total          │
│   late_arrival_count, early_departure_count, overtime_count                │
│                                                                             │
│   employee_attendance_metrics                                              │
│   ──────────────────────────                                                │
│   id (PK)                                                                  │
│   client_employee_id, year, month                                          │
│   total_shifts, days_worked, total_hours_worked                             │
│   late_arrival_count/rate, early_departure_count/rate                       │
│   overtime_count/rate, avg_variance_minutes                                 │
│   rolling_avg_hours_4w                                                      │
│                                                                             │
│   department_monthly_metrics                                                │
│   ─────────────────────────                                                  │
│   id (PK)                                                                  │
│   department_id, department_name, year, month                              │
│   active_headcount, total_hires, total_terminations                        │
│   turnover_rate, avg_tenure_days, avg_weekly_hours                         │
│   late_arrival_rate, early_departure_rate, overtime_rate                   │
│                                                                             │
│   headcount_trend                                                           │
│   ───────────────                                                           │
│   id (PK)                                                                  │
│   year, month                                                               │
│   active_headcount, new_hires, terminations                                │
│   early_attrition_count, early_attrition_rate                              │
│                                                                             │
│   organization_metrics                                                      │
│   ───────────────────                                                       │
│   id (PK)                                                                  │
│   organization_id, organization_name, year, month                          │
│   total_employees, active_employees, total_departments                    │
│   avg_tenure_days, turnover_rate                                           │
│   avg_late_arrival_rate, avg_early_departure_rate, avg_overtime_rate       │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Schema Summary

| Layer | Tables | Purpose |
|-------|--------|---------|
| **bronze** | `employee_raw`, `timesheet_raw` | Raw data ingestion |
| **silver** | `organization`, `department`, `employee`, `timesheet` | Cleaned, normalized data |
| **gold** | 6 tables | Pre-computed analytics |

### Key Relationships

- **Employee → Organization**: Many-to-One
- **Employee → Department**: Many-to-One
- **Employee → Employee** (manager): Self-referencing Many-to-One
- **Employee → Timesheet**: One-to-Many
- **Department → Organization**: Many-to-One
- **Gold tables**: Denormalized snapshots - no foreign keys, rebuilt monthly
