import os
import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

# Добавляем папку текущего DAG в sys.path для корректного импорта из subtask_scripts
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
if CURRENT_DIR not in sys.path:
    sys.path.insert(0, CURRENT_DIR)

from subtask_scripts.delivery_tasks import (
    extract_from_rabbit_to_s3,
    load_from_s3_to_clickhouse,
)

default_args = {
    "owner": "egiazarian_aa",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="sd_eaa_delivery_etl",
    default_args=default_args,
    description="ETL-поток: RabbitMQ -> S3 -> ClickHouse",
    schedule_interval=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["egiazarian_aa", "hw_9"],
) as dag:

    task_extract_to_s3 = PythonOperator(
        task_id="extract_rabbit_to_s3",
        python_callable=extract_from_rabbit_to_s3,
    )

    task_load_to_ch = PythonOperator(
        task_id="load_s3_to_clickhouse",
        python_callable=load_from_s3_to_clickhouse,
    )

    task_extract_to_s3 >> task_load_to_ch
