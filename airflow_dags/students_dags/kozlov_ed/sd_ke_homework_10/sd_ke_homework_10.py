import os
import sys
from datetime import datetime, timedelta, timezone
from airflow import DAG
from airflow.operators.python import PythonOperator

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from sd_ke_homework_10.subtask_scripts.tasks import rabbit_to_s3, refresh_dimensions, load_facts, check_dimensions

with DAG(
    dag_id='sd_ke_homework_10',
    start_date=datetime(2026, 11, 1, tzinfo=timezone.utc),
    schedule=None, catchup=False, max_active_runs=1,
    default_args={'owner': 'kozlov_ed', 'retries': 2,
                  'retry_delay': timedelta(minutes=1), 'pool': 'default_pool'},
    tags=['kozlov_ed', 'hw_10'],
) as dag:
    extract = PythonOperator(task_id='rabbit_to_s3', python_callable=rabbit_to_s3)
    refresh = PythonOperator(task_id='refresh_dimensions', python_callable=refresh_dimensions)
    facts = PythonOperator(task_id='load_facts', python_callable=load_facts)
    check = PythonOperator(task_id='check_dimensions', python_callable=check_dimensions)
    extract >> refresh >> facts >> check
