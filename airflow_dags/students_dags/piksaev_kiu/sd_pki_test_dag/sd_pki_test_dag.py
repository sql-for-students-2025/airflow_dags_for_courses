import datetime
import os
import sys

from airflow import DAG
from airflow.operators.python import PythonOperator

sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))

from subtask_scripts import sd_pki_test_task

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
    dag_id="sd_pki_test_dag",
    default_args=default_args,
    schedule=None,
    start_date=datetime.datetime(2020, 7, 1, 0, 0),
    catchup=False,
    tags=["piksaev_kiu", "homework_9", "test"],
) as dag:

    task_1 = PythonOperator(
        task_id="test_wo_arg",
        python_callable=sd_pki_test_task.test_python,
        dag=dag,
    )

    task_2 = PythonOperator(
        task_id="test_with_arg",
        python_callable=sd_pki_test_task.test_python,
        op_kwargs={"arg": "Unknown"},
        dag=dag,
    )

    task_3 = PythonOperator(
        task_id="test_from_subtask",
        python_callable=sd_pki_test_task.task_from_folder,
        dag=dag,
    )

    task_1 >> task_2 >> task_3
