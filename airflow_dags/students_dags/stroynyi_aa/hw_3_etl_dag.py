from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from subtask_scripts.rabbit_consumer import consume_rabbitmq_to_file
from subtask_scripts.s3_to_clickhouse import upload_to_s3, load_s3_to_clickhouse

default_args = {
    "owner": "stroinyi_aa",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="hw_3_etl_sales",
    description="RabbitMQ -> S3 -> ClickHouse",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    default_args=default_args,
    tags=["stroinyi_aa", "hw_3", "etl", "sales"],
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