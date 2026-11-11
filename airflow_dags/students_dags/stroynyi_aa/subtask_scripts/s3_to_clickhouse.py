import os
import boto3
import urllib3
from airflow.models import Variable
from clickhouse_driver import Client

urllib3.disable_warnings()

S3_BUCKET = "dad-ksu-datalab"
S3_ENDPOINT = "https://storage.yandexcloud.net"
MY_LOGIN = "stroinyi_aa"  
CH_HOST = "111.88.243.32" 

def upload_to_s3(**context) -> str:
    """Кладёт локальный NDJSON в S3: DATA/stroinyi_aa/<run_id>.json"""
    access_key = Variable.get("s3_dad_ksu_dataplatform_access_key")
    secret_key = Variable.get("s3_dad_ksu_dataplatform_secret_key")

    local_file = context["ti"].xcom_pull(
        task_ids="consume_rabbitmq", key="local_file"
    )
    if not local_file or not os.path.exists(local_file):
        raise FileNotFoundError(f"Нет файла из предыдущей задачи: {local_file}")

    run_id = context["run_id"].replace(":", "_").replace("+", "_")
    s3_key = f"DATA/{MY_LOGIN}/{run_id}.json"

    s3 = boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        verify=False
    )
    s3.upload_file(local_file, S3_BUCKET, s3_key)

    context["ti"].xcom_push(key="s3_key", value=s3_key)
    return s3_key


def load_s3_to_clickhouse(**context) -> None:
    """Читает файл из S3 и грузит в ClickHouse."""
    access_key = Variable.get("s3_dad_ksu_dataplatform_access_key")
    secret_key = Variable.get("s3_dad_ksu_dataplatform_secret_key")
    ch_user = Variable.get("clickhouse_tech_user_account")
    ch_pass = Variable.get("clickhouse_tech_user_password")

    s3_key = context["ti"].xcom_pull(task_ids="upload_to_s3", key="s3_key")

    s3 = boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        verify=False
    )
    tmp_path = f"/tmp/{MY_LOGIN}_batch.json"
    s3.download_file(S3_BUCKET, s3_key, tmp_path)

    with open(tmp_path, "r", encoding="utf-8") as f:
        raw_text = f.read()

    with Client(
        host=CH_HOST,
        user=ch_user,
        password=ch_pass,
        database=f"{MY_LOGIN}_db",
        port=9000,
    ) as client:
        # 1. Грузим в стейджинг.
        # Используем f-строку и передаем данные через f-string, 
        # чтобы драйвер не пытался их парсить как параметры.
        # Это самый надежный способ для JSONEachRow.
        insert_query = f"INSERT INTO fact_orders_raw FORMAT JSONEachRow\n{raw_text}"
        client.execute(insert_query)
        
        # 2. Перекладываем в витрину
        client.execute(
            """
            INSERT INTO fact_orders
            SELECT
                event_id                                    AS order_id,
                toDate(parseDateTimeBestEffort(event_time)) AS event_date,
                customer_id,
                receipt_status,
                store_city,
                store_region,
                product_id,
                product_category,
                product_brand,
                toUInt32(quantity)                          AS quantity,
                toDecimal64(gross_amount, 2)                AS gross_amount,
                toDecimal64(discount_amount, 2)             AS discount_amount,
                toDecimal64(net_amount, 2)                  AS net_amount
            FROM fact_orders_raw
            WHERE event_id NOT IN (SELECT order_id FROM fact_orders)
            """
        )