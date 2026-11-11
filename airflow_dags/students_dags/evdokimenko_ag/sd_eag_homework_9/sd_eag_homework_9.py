import datetime
import os
import sys

from airflow import DAG
from airflow.operators.python import PythonOperator

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), 'subtask_scripts'))
import eag_supply_etl

default_args = {
    'owner': 'evdokimenko_ag',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=2),
}

with DAG(
    dag_id='sd_eag_homework_9',
    default_args=default_args,
    description='RabbitMQ supply.queue -> S3 -> ClickHouse (витрина поставок)',
    schedule=None,
    start_date=datetime.datetime(2026, 11, 1),
    catchup=False,
    max_active_runs=1,
    tags=['evdokimenko_ag', 'homework_9'],
) as dag:
    rabbit_to_s3 = PythonOperator(
        task_id='rabbit_to_s3',
        python_callable=eag_supply_etl.rabbit_to_s3,
    )

    s3_to_clickhouse = PythonOperator(
        task_id='s3_to_clickhouse',
        python_callable=eag_supply_etl.s3_to_clickhouse,
    )

    rabbit_to_s3 >> s3_to_clickhouse
