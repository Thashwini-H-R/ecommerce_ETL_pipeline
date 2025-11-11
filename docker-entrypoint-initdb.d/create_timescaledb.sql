-- create_timescaledb.sql
-- This script will be executed by the Postgres image during initial DB initialization.
-- It creates the TimescaleDB extension in the database specified by POSTGRES_DB (e.g. `airflow`).

CREATE EXTENSION IF NOT EXISTS timescaledb;
