import datetime
import os
import sys

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'subtask_scripts'))
from sd_kmiu_test_tasks import test_success, task_from_folder


with DAG(
    dag_id='sd_kmiu_test_dag',
    default_args={'owner': 'kochetkova_miu', 'retries': 0},
    schedule=None,
    start_date=datetime.datetime(2026, 1, 1),
    catchup=False,
    tags=['kochetkova_miu', 'test', 'cw'],
) as dag:
    task_1 = PythonOperator(
        task_id='test_wo_arg',
        python_callable=test_success,
    )

    task_2 = PythonOperator(
        task_id='test_with_arg',
        python_callable=test_success,
        op_kwargs={'arg': 'kochetkova_miu'},
    )

    task_3 = PythonOperator(
        task_id='test_from_subtask',
        python_callable=task_from_folder,
    )

    task_1 >> task_2 >> task_3