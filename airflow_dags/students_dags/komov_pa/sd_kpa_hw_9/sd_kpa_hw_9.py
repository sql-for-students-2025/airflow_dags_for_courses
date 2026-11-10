from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

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

    task_extract = PythonOperator(
        task_id='extract_to_s3',
        python_callable=extract_rabbitmq_to_s3
    )

    task_load = PythonOperator(
        task_id='load_to_clickhouse',
        python_callable=load_s3_to_clickhouse
    )

    task_extract >> task_load