import datetime
import os
import sys

from airflow import DAG
from airflow.operators.python import PythonOperator

sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))

from subtask_scripts import sd_nsv_movement_etl

default_args = {
    "owner": "nechaev_sv",
    "depends_on_past": False,
    "start_date": datetime.datetime(2026, 11, 1, 0, 0),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": datetime.timedelta(minutes=1),
    "pool": "default",
}

with DAG(
    dag_id="sd_nsv_homework_9",
    default_args=default_args,
    schedule=None,
    start_date=datetime.datetime(2026, 11, 1, 0, 0),
    catchup=False,
    tags=["nechaev_sv", "homework_9"],
) as dag:

    task_rabbitmq_to_s3 = PythonOperator(
        task_id="rabbitmq_to_s3",
        python_callable=sd_nsv_movement_etl.rabbitmq_to_s3,
        dag=dag,
    )

    task_s3_to_clickhouse = PythonOperator(
        task_id="s3_to_clickhouse",
        python_callable=sd_nsv_movement_etl.s3_to_clickhouse,
        dag=dag,
    )

    task_rabbitmq_to_s3 >> task_s3_to_clickhouse
