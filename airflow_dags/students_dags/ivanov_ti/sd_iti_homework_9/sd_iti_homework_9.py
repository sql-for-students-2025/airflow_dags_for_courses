from airflow import DAG
from airflow.operators.python import PythonOperator
import datetime
import sys, os

sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))
from subtask_scripts import sd_iti_homework_9_tasks

default_args = {
    'owner': 'ivanov_ti',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': datetime.timedelta(minutes=1),
}

with DAG(
    dag_id="sd_iti_homework_9",
    default_args=default_args,
    schedule=None,
    start_date=datetime.datetime(2026, 11, 1),
    catchup=False,
    tags=["ivanov_ti", "hw_9"],
) as dag:

    rabbit_to_s3 = PythonOperator(
        task_id='rabbit_to_s3',
        python_callable=sd_iti_homework_9_tasks.rabbit_to_s3,
        dag=dag
    )

    s3_to_clickhouse = PythonOperator(
        task_id='s3_to_clickhouse',
        python_callable=sd_iti_homework_9_tasks.s3_to_clickhouse,
        dag=dag
    )

    rabbit_to_s3 >> s3_to_clickhouse
