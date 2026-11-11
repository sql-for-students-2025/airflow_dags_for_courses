import datetime
import os
import sys

from airflow import DAG
from airflow.operators.python import PythonOperator

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "subtask_scripts"))

from sd_pki_extract_to_s3 import extract_rabbitmq_to_s3
from sd_pki_load_to_clickhouse import load_s3_to_clickhouse

default_args = {
    "owner": "piksaev_kiu",
    "depends_on_past": False,
    "start_date": datetime.datetime(2020, 7, 1, 0, 0),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": datetime.timedelta(minutes=1),
    "pool": "default",
}

with DAG(
    dag_id="sd_pki_homework_9",
    default_args=default_args,
    schedule=None,
    start_date=datetime.datetime(2020, 7, 1, 0, 0),
    catchup=False,
    tags=["piksaev_kiu", "homework_9"],
) as dag:

    task_extract = PythonOperator(
        task_id="extract_rabbitmq_to_s3",
        python_callable=extract_rabbitmq_to_s3,
        dag=dag,
    )

    task_load = PythonOperator(
        task_id="load_s3_to_clickhouse",
        python_callable=load_s3_to_clickhouse,
        dag=dag,
    )

    task_extract >> task_load
