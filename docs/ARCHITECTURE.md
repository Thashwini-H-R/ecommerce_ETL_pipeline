## Architecture overview

This repository implements a modular ETL pipeline for e-commerce data. Key components:

- Ingest: `ingest.py` pulls raw data from external sources (Shopify, WooCommerce, Stripe, PayPal, inventory DB) and writes raw payloads to a local `staging/` folder (or S3 when configured).
- Transform: `transform.py` contains composable functions for cleaning, deduplication, currency normalization (with optional live FX), datetime normalization, fraud scoring and derived metrics such as CLV.
- Load: `loader.py` performs efficient batch upserts into a Postgres/TimescaleDB warehouse using `psycopg2.extras.execute_values` and `ON CONFLICT DO UPDATE` semantics.
- Orchestration: `dags/etl_pipeline.py` is an Airflow DAG (PythonOperators) wiring ingest -> transform -> load with bookmarks for incremental ingestion and simple failure notifications.
- Serving & Dashboard: `api/app.py` exposes key queries and aggregated metrics; `dashboard/streamlit_app.py` demonstrates a small front-end.

Storage & analytics
- The warehouse schema (in `sql/warehouse_schema.sql`) defines dimension tables (`customers_dim`, `products_dim`) and fact tables (`orders_fact`, `transactions_fact`).
- TimescaleDB hypertables are used for the time-series facts for performant analytics over order/transaction timelines.

Testing & validation
- Unit tests are under `tests/` and run with `pytest` (a Makefile target `make test` has been provided).
- `validate.py` implements data integrity checks that can be run standalone or integrated into the DAG prior to load.

Deployment notes
- On Windows, Airflow is best run via Docker/WSL2. See `DEVELOPMENT.md` for commands to build images and start the compose stack.
