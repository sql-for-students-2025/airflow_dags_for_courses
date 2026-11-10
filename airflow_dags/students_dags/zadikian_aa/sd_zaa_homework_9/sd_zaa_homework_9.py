import datetime
import os
import sys

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'subtask_scripts'))
from sd_zaa_sales_tasks import rabbitmq_to_s3, s3_to_clickhouse


with DAG(
    dag_id='sd_zaa_homework',
    default_args={'owner': 'zadikian_aa', 'retries': 0},
    schedule=None,
    start_date=datetime.datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=['zadikian_aa', 'homework', 'hw_1'],
) as dag:
    task_1 = PythonOperator(
        task_id='rabbitmq_to_s3',
        python_callable=rabbitmq_to_s3,
    )

    task_2 = PythonOperator(
        task_id='s3_to_clickhouse',
        python_callable=s3_to_clickhouse,
    )

    task_1 >> task_2
