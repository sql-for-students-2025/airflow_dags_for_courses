import datetime
import os
import sys
from airflow import DAG
# Используем старый импорт для Airflow 2 (как на сервере)
from airflow.operators.python_operator import PythonOperator

# Добавляем путь к скриптам в подпапке
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'subtask_scripts'))
from rabbit_to_s3 import fetch_rabbit_to_s3
from s3_to_clickhouse import load_s3_to_clickhouse

with DAG(
    dag_id='sd_iad_homework_9',
    default_args={'owner': 'ivanenko_ad', 'retries': 0
                , 'retry_delay': datetime.timedelta(minutes=1)},
    schedule=None,
    start_date=datetime.datetime(2024, 1, 1),
    catchup=False,
    tags=['ivanenko_ad', 'homework_9', 'Alex_Dikiy'],
) as dag:


    task_fetch = PythonOperator(
        task_id='fetch_rabbit_to_s3',
        python_callable=fetch_rabbit_to_s3,
        op_kwargs={'login': 'ivanenko_ad'},
    )


    task_load = PythonOperator(
        task_id='load_s3_to_clickhouse',
        python_callable=load_s3_to_clickhouse,
        op_kwargs={'s3_path': "{{ ti.xcom_pull(task_ids='fetch_rabbit_to_s3') }}"},
    )

    # Последовательность
    task_fetch >> task_load