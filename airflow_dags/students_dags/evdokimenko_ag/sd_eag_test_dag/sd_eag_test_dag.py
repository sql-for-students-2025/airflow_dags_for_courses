import datetime
import os
import sys

from airflow import DAG
from airflow.operators.python import PythonOperator

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), 'subtask_scripts'))
import eag_test_subtask

default_args = {
    'owner': 'evdokimenko_ag',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': datetime.timedelta(minutes=1),
}

with DAG(
    dag_id='sd_eag_test_dag',
    default_args=default_args,
    schedule=None,
    start_date=datetime.datetime(2026, 11, 1),
    catchup=False,
    tags=['evdokimenko_ag', 'homework_9', 'test'],
) as dag:
    task_1 = PythonOperator(
        task_id='test_wo_arg',
        python_callable=eag_test_subtask.say_hello,
    )

    task_2 = PythonOperator(
        task_id='test_with_arg',
        python_callable=eag_test_subtask.say_hello,
        op_kwargs={'arg': 'evdokimenko_ag'},
    )

    task_3 = PythonOperator(
        task_id='test_from_subtask',
        python_callable=eag_test_subtask.task_from_folder,
    )

    task_1 >> task_2 >> task_3
