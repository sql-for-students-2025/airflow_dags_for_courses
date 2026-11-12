import sys
from pathlib import Path
from datetime import datetime, timezone
from airflow import DAG
from airflow.operators.python import PythonOperator
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from sd_ke_test_homework_9.subtask_scripts.tasks import test_python

with DAG(
    dag_id="sd_ke_test_homework_9", schedule=None, catchup=False,
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    default_args={"owner": "kozlov_ed", "pool": "default_pool"},
    tags=["kozlov_ed", "hw_9", "test"],
) as dag:
    first = PythonOperator(task_id="test_wo_arg", python_callable=test_python)
    second = PythonOperator(task_id="test_with_arg", python_callable=test_python,
                            op_kwargs={"arg": "kozlov_ed"})
    third = PythonOperator(task_id="test_from_subtask", python_callable=test_python,
                           op_kwargs={"arg": "subtask_scripts"})
    first >> second >> third
