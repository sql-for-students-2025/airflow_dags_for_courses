import Path
from datetime import datetime, timedelta, timezone
from airflow import DAG
from airflow.operators.python import PythonOperator


sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from sd_ke_homework_9.subtask_scripts.tasks import rabbit_to_s3, s3_to_clickhouse

with DAG(
    dag_id="sd_ke_homework_9",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    schedule=None, catchup=False, max_active_runs=1,
    default_args={"owner": "kozlov_ed", "retries": 2,
                  "retry_delay": timedelta(minutes=1), "pool": "default_pool"},
    tags=["kozlov_ed", "hw_9"],
) as dag:
    extract = PythonOperator(task_id="rabbit_to_s3", python_callable=rabbit_to_s3)
    load = PythonOperator(task_id="s3_to_clickhouse", python_callable=s3_to_clickhouse)
    extract >> load
