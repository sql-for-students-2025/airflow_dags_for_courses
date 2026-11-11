import datetime
import os
import sys
from airflow import DAG
from airflow.operators.python import PythonOperator

sys.path.insert(0, '/opt/airflow/dags/repo/airflow_dags/students_dags/kochetkova_miu/sd_kmiu_homework_9/subtask_scripts')
from sd_kmiu_supply_tasks import rabbitmq_to_s3, s3_to_clickhouse

default_args = {
    'owner': 'kochetkova_miu',
    'depends_on_past': False,
    'start_date': datetime.datetime(2026, 11, 1),
    'retries': 0,
    'retry_delay': datetime.timedelta(minutes=1),
}

with DAG(
    dag_id='sd_kmiu_homework_9',
    default_args=default_args,
    schedule=None,
    catchup=False,
    tags=['kochetkova_miu', 'hw_9'],
) as dag:

    task_rabbitmq_to_s3 = PythonOperator(
        task_id='rabbitmq_to_s3_task',
        python_callable=rabbitmq_to_s3,
    )

    task_s3_to_clickhouse = PythonOperator(
        task_id='s3_to_clickhouse_task',
        python_callable=s3_to_clickhouse,
    )

    task_rabbitmq_to_s3 >> task_s3_to_clickhouse
