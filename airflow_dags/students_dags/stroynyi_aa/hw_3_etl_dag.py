import sys
import os
from datetime import datetime, timedelta

# --- Блок автоустановки библиотек для Airflow ---
# Airflow на сервере может не иметь этих библиотек, поэтому мы их доустанавливаем
def install_packages():
    import subprocess
    packages = ["pika", "clickhouse-driver", "boto3"]
    for pkg in packages:
        try:
            __import__(pkg.replace("-", "_"))
        except ImportError:
            subprocess.check_call([sys.executable, "-m", "pip", "install", pkg])

install_packages()
# ------------------------------------------------

from airflow import DAG
from airflow.operators.python import PythonOperator

# Добавляем путь к текущей папке, чтобы Airflow нашел папку subtask_scripts
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))

from subtask_scripts.rabbit_consumer import consume_rabbitmq_to_file
from subtask_scripts.s3_to_clickhouse import upload_to_s3, load_s3_to_clickhouse

default_args = {
    "owner": "stroinyi_aa",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="sd_saa_homework_9",  # Переименовали в общий формат (saa - stroynyi_aa)
    description="RabbitMQ -> S3 -> ClickHouse",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    default_args=default_args,
    tags=["homework_9", "stroinyi_aa", "etl"], # Добавили обязательный тег homework_9
) as dag:

    t1 = PythonOperator(
        task_id="consume_rabbitmq",
        python_callable=consume_rabbitmq_to_file,
        provide_context=True,
    )
    t2 = PythonOperator(
        task_id="upload_to_s3",
        python_callable=upload_to_s3,
        provide_context=True,
    )
    t3 = PythonOperator(
        task_id="load_s3_to_clickhouse",
        python_callable=load_s3_to_clickhouse,
        provide_context=True,
    )

    t1 >> t2 >> t3