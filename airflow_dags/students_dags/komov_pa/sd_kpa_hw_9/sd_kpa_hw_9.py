import os
import sys
from datetime import datetime, timedelta
from airflow import DAG

from airflow.operators.python import PythonVirtualenvOperator
from airflow.models import Variable

current_dir = os.path.dirname(os.path.abspath(__file__))
if current_dir not in sys.path:
    sys.path.insert(0, current_dir)

from subtask_scripts.sd_kpa_tasks import extract_rabbitmq_to_s3, load_s3_to_clickhouse

default_args = {
    'owner': 'komov_pa',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='sd_kpa_hw_9',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=['komov_pa', 'hw_9']
) as dag:


    task_extract = PythonVirtualenvOperator(
        task_id='extract_to_s3',
        python_callable=extract_rabbitmq_to_s3,
        requirements=["pika", "pandas", "clickhouse-connect", "apache-airflow"],
        system_site_packages=False,
    )


    task_load = PythonVirtualenvOperator(
        task_id='load_to_clickhouse',
        python_callable=load_s3_to_clickhouse,
        requirements=["pika", "pandas", "clickhouse-connect", "apache-airflow"],
        system_site_packages=False,
    )

    task_extract >> task_load