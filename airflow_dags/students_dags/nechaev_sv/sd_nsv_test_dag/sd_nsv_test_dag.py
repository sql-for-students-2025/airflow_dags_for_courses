import datetime
import os
import sys

from airflow import DAG
from airflow.operators.python import PythonOperator

sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))

from subtask_scripts import sd_nsv_test_tasks

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
    dag_id="sd_nsv_test_dag",
    default_args=default_args,
    schedule=None,
    start_date=datetime.datetime(2026, 11, 1, 0, 0),
    catchup=False,
    tags=["nechaev_sv", "homework_9", "test"],
) as dag:

    task_hello = PythonOperator(
        task_id="hello",
        python_callable=sd_nsv_test_tasks.hello,
        op_kwargs={"name": "nechaev_sv"},
        dag=dag,
    )

    task_check_environment = PythonOperator(
        task_id="check_environment",
        python_callable=sd_nsv_test_tasks.check_environment,
        dag=dag,
    )

    task_hello >> task_check_environment
