from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator
import datetime
import sys, os

sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))
from subtask_scripts import zva_supply_etl


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


with DAG(
    dag_id="sd_zva_homework_9",
    default_args=default_args,
    schedule=None,
    start_date=days_ago(0),
    catchup=False,
    tags=["zlatkin_va", "homework_9"],
) as dag:

    rabbit_to_s3 = PythonOperator(
        task_id='rabbit_to_s3',
        python_callable=zva_supply_etl.rabbit_to_s3,
        dag=dag
    )

    s3_to_clickhouse = PythonOperator(
        task_id='s3_to_clickhouse',
        python_callable=zva_supply_etl.s3_to_clickhouse,
        dag=dag
    )

    rabbit_to_s3 >> s3_to_clickhouse