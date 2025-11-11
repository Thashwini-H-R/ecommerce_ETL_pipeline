Development setup

This project uses a Python virtual environment. These instructions assume you're on Windows PowerShell (the project was set up there). Adjust paths for other shells.

Activate venv (PowerShell)

```powershell
cd 'C:\Users\dell\OneDrive\Desktop\ecommerce_ETL_pipeline'
.\venv\Scripts\Activate.ps1
```

If script execution is blocked, allow signed scripts for your user:

```powershell
Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser
.\venv\Scripts\Activate.ps1
```

Common setup steps

```powershell
# upgrade pip and tools
python -m pip install --upgrade pip setuptools wheel
# install project deps from pinned file
python -m pip install -r requirements.txt
```

Quick verification (run after activation)

```powershell
python -c "import airflow; print('airflow', airflow.__version__)"
python -c "import psycopg2; print('psycopg2', psycopg2.__version__)"
python -c "import timescaledb; print('timescaledb', getattr(timescaledb, '__version__', 'n/a'))"
python -c "import pandas as pd; print('pandas', pd.__version__)"
python -c "import requests; print('requests', requests.__version__)"
```

Notes

- Apache Airflow: Airflow installs in the venv but is not officially supported on native Windows. For local development, prefer using Docker or WSL2. See https://airflow.apache.org/docs/
- TimescaleDB: the `timescaledb` Python package is a helper. You still need a PostgreSQL server with the TimescaleDB extension to use TimescaleDB features (can run via Docker).
- The `requirements.txt` in the repository is generated from the venv (pinned). Re-run `python -m pip freeze > requirements.txt` after adding/removing dependencies.

Running with Docker Compose (Postgres + TimescaleDB + Airflow)

This repository includes a minimal `docker-compose.yml` that starts:
- a TimescaleDB-enabled Postgres database (service `postgres`)
- an Airflow init job to initialize the metadata DB and create an admin user (service `airflow-init`)
- an Airflow webserver (service `airflow-webserver`) exposed on port 8080
- an Airflow scheduler (service `airflow-scheduler`)

Files/directories mounted into the Airflow containers:
- `./dags` -> `/opt/airflow/dags` (create this folder and add your DAGs)
- `./logs` -> `/opt/airflow/logs`
- `./plugins` -> `/opt/airflow/plugins`

Start the stack (requires Docker and Docker Compose):

```powershell
# From the project root
docker compose up airflow-init  # runs DB migrations and creates the admin user
docker compose up -d postgres airflow-webserver airflow-scheduler

# Then view the UI at http://localhost:8080 (username: admin, password: admin)
```

Notes and caveats

- The `timescale/timescaledb` image provides the TimescaleDB extension for Postgres. To use hypertables, enable the extension inside the `airflow` DB (e.g. `CREATE EXTENSION IF NOT EXISTS timescaledb;`).
- Airflow is still best exercised on Unix-like hosts (Linux/WSL2/Docker). This compose is intended for development via Docker on Windows.
- If you want a more production-like Airflow setup (Celery/Kubernetes executors, Redis, flower, worker autoscaling, traefik, persistent volumes), I can add a more complete `docker-compose.yaml` following the official Airflow compose examples.

Helper scripts

I added a `Makefile` and a PowerShell helper at `scripts/dev.ps1` to simplify running the compose stack.

Makefile targets:
- `make init` — run the `airflow-init` service to initialize the DB and create the admin user
- `make up` — start the stack in the background (postgres, redis, webserver, scheduler, worker)
- `make down` — stop the stack and remove volumes
- `make logs` — tail the logs
- `make ps` — show containers

PowerShell helper (Windows):

```powershell
# usage (from project root)
# init
.\n+scripts\dev.ps1 -Action init
# up
.
scripts\dev.ps1 -Action up
# logs
.
scripts\dev.ps1 -Action logs
```

Sample DAG

A minimal DAG is included at `dags/sample_dag.py`. It runs a PythonOperator that prints a message daily. After starting the stack and confirming the scheduler is running, you should see the DAG in the Airflow UI and the task runs in the worker logs.

Building custom images (optional)

If you prefer to build custom images instead of using the official images from Docker Hub, two Dockerfiles have been added:

- `Dockerfile.airflow` — builds an Airflow image from `apache/airflow:3.1.2`, installs `requirements.txt` (if present), and copies the repository `dags/` and `plugins/` into the image.
- `Dockerfile.postgres` — extends the TimescaleDB image and includes SQL initialization scripts under `docker-entrypoint-initdb.d/` (a script to create the TimescaleDB extension is included).

Build example (from project root):

```powershell
# build images
docker build -t my-airflow:3.1.2 -f Dockerfile.airflow .
docker build -t my-timescaledb:pg15 -f Dockerfile.postgres .

# then update docker-compose.yml services to use `image: my-airflow:3.1.2` and `image: my-timescaledb:pg15`
```

Note: The repo already includes a working `docker-compose.yml` that pulls official images. Building custom images can be useful when you want a self-contained image with pinned dependencies and bundled DAGs/plugins.

