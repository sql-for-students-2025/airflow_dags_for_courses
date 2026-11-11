from airflow import DAG
from airflow.operators.python import PythonOperator
import datetime
import sys, os

sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))
from subtask_scripts import sd_iti_test_dag_tasks

default_args = {
    'owner': 'ivanov_ti',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': datetime.timedelta(minutes=1),
}


def test_python(arg='User'):
    print(f'SUCCESS {arg}!!!')


with DAG(
    dag_id="sd_iti_test_dag",
    default_args=default_args,
    schedule=None,
    start_date=datetime.datetime(2026, 11, 1),
    catchup=False,
    tags=["ivanov_ti", "test", "Homework_9"],
) as dag:

    task_1 = PythonOperator(
        task_id='test_wo_arg',
        python_callable=test_python,
        dag=dag
    )

    task_2 = PythonOperator(
        task_id='test_with_arg',
        python_callable=test_python,
        op_kwargs={'arg': 'Unknown'},
        dag=dag
    )

    task_3 = PythonOperator(
        task_id='test_from_subtask',
        python_callable=sd_iti_test_dag_tasks.task_from_folder,
        dag=dag
    )

    task_1 >> task_2 >> task_3
