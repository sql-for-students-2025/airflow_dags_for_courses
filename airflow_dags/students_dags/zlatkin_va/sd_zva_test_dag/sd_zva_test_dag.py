from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator
import datetime
import sys, os

sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))
from subtask_scripts import subtask_from_folder


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime.datetime(2020, 7, 1, 0, 0),
    'email': ['your_email@domen'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': datetime.timedelta(minutes=1),
    'pool': 'default'
}


def test_python(arg='User'):
    print(f'SUCCESS {arg}!!!')


with DAG(
    dag_id="sd_zva_test_dag",
    default_args=default_args,
    schedule=None,
    start_date=days_ago(0),
    catchup=False,
    tags=["zlatkin_va", "homework_9"],
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
        task_id='test_from_subrtask',
        python_callable=subtask_from_folder.task_from_folder,
        dag=dag
    )

    task_1 >> task_2 >> task_3