## Module reference

This file lists the main modules and a short description of their responsibilities.

- `ingest.py` — connectors for external sources and a `Storage` helper to write raw payloads to `staging/` or S3.
- `transform.py` — data cleaning and normalization utilities. Small composable functions suitable for use in Airflow tasks.
- `loader.py` — batch upserts into Postgres/TimescaleDB using `psycopg2` and `execute_values`.
- `bookmarks.py` — JSON-backed simple bookmark store used for incremental ingestion checkpoints.
- `dags/etl_pipeline.py` — Airflow DAG wiring ingest->transform->load, with per-source parsing and validation integration points.
- `sql/warehouse_schema.sql` — SQL DDL to create dimensions and facts and Timescale hypertables.
- `api/app.py` — FastAPI application exposing warehouse queries and an orders-per-day metric endpoint for dashboards.
- `dashboard/streamlit_app.py` — Streamlit demo dashboard that reads the API and visualizes trends.
- `validate.py` — validation utilities to assert data completeness and basic integrity before loading.

Refer to the inline docstrings in each module for more details and examples.
