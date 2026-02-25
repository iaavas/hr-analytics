# HR Insights ETL Pipeline

## Quick Start
1) `python -m src.app.database` to create schemas/tables (or `alembic upgrade head`).
2) Run the full pipeline: `python -m luigi --module src.etl.gold.tasks LoadAllGold --local-scheduler --all-months True`
3) Serve API: `uvicorn src.app.main:app --reload --port 5173`
4) Generate visuals (after gold data is populated): `python dashboard/visualize.py` – HTML files land in `dashboard/output/`.

## Analytics SQL
Pre-built KPI queries live at `src/etl/gold/metrics/kpis.sql` and target the gold tables produced by the ETL.
