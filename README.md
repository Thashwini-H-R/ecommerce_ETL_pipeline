# ecommerce_ETL_pipeline

This repository contains a small e-commerce ETL pipeline built for local development.

Contents (high level):

- `ingest.py` — connectors and staging helpers for Shopify, WooCommerce, Stripe, PayPal, and inventory DB snapshots.
- `transform.py` — data cleaning, normalization, currency conversion, CLV, fraud flags and validations.
- `loader.py` — batch upsert loader into Postgres/TimescaleDB using psycopg2.
- `dags/etl_pipeline.py` — Airflow DAG that orchestrates ingest -> transform -> load.
- `sql/warehouse_schema.sql` — warehouse tables and TimescaleDB hypertable creation.
- `api/app.py` — FastAPI service exposing warehouse data for dashboards and BI.
- `dashboard/streamlit_app.py` — simple Streamlit dashboard using the API.
- `tests/` — pytest unit tests for transform and validation utilities.

See `docs/` for architecture, workflows, and developer guidance.

Quickstart
1. Create and activate the venv, install requirements (see `DEVELOPMENT.md`).
2. Run the tests: `make test` (uses the venv's pytest on Windows).
3. Start services via Docker Compose (Postgres/Timescale, Redis, Airflow) or run the API and Streamlit locally.
