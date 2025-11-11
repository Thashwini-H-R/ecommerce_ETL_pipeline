from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator


def hello_task(**context):
    print('Hello from sample DAG!')


with DAG(
    dag_id='sample_hello_dag',
    start_date=datetime(2025, 1, 1),
    schedule_interval='@daily',
    catchup=False,
    default_args={'retries': 1, 'retry_delay': timedelta(minutes=1)},
) as dag:
    t1 = PythonOperator(task_id='say_hello', python_callable=hello_task)

    t1
