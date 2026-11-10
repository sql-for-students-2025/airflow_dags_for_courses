import datetime
import os
import sys
from airflow import DAG
from airflow.operators.python import PythonOperator

sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'subtask_scripts'))

from sd_kpa_tasks import extract_rabbitmq_to_s3, load_s3_to_clickhouse

with DAG(
    dag_id='sd_kpa_hw_9',
    default_args={'owner': 'komov_pa', 'retries': 0},
    schedule=None,
    start_date=datetime.datetime(2026, 1, 1),
    catchup=False,
    tags=['komov_pa', 'hw_9'],
) as dag:

    task_extract = PythonOperator(
        task_id='extract_to_s3',
        python_callable=extract_rabbitmq_to_s3,
    )

    task_load = PythonOperator(
        task_id='load_to_clickhouse',
        python_callable=load_s3_to_clickhouse,
    )

    task_extract >> task_load