from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
import boto3
import urllib3

urllib3.disable_warnings()

def _test_s3():
    s3 = boto3.client(
        "s3",
        endpoint_url="https://storage.yandexcloud.net", # Из методички
        aws_access_key_id=Variable.get("s3_dad_ksu_dataplatform_access_key"),
        aws_secret_access_key=Variable.get("s3_dad_ksu_dataplatform_secret_key"),
        verify=False
    )
    s3.put_object(
        Bucket="dad-ksu-datalab", # Из методички
        Key="DATA/stroinyi_aa/_test.txt", # Ваш логин
        Body=b"hello from airflow",
    )

with DAG(
    dag_id="hw_3_test_s3",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["stroinyi_aa", "hw_3", "test"],
) as dag:
    PythonOperator(task_id="test_s3", python_callable=_test_s3)