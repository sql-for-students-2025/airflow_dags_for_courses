import os

import boto3
from airflow.models import Variable
from clickhouse_driver import Client

S3_BUCKET = "dataplatform-bucket"   # уточните имя в cw.md
S3_ENDPOINT = "https://storage.example"  # уточните в cw.md
MY_LOGIN = "your_login"             # ← заменить


def upload_to_s3(**context) -> str:
    """Кладёт локальный NDJSON в S3: DATA/your_login/<run_id>.json"""
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
    )
    s3.upload_file(local_file, S3_BUCKET, s3_key)

    context["ti"].xcom_push(key="s3_key", value=s3_key)
    return s3_key


def load_s3_to_clickhouse(**context) -> None:
    """
    Читает файл из S3 и грузит в ClickHouse.
    Проще всего — скачать объект во временный файл и сделать INSERT ... FORMAT JSONEachRow.
    """
    access_key = Variable.get("s3_dad_ksu_dataplatform_access_key")
    secret_key = Variable.get("s3_dad_ksu_dataplatform_secret_key")
    ch_host = Variable.get("clickhouse_port")       # в задании так названо
    ch_user = Variable.get("clickhouse_tech_user_account")
    ch_pass = Variable.get("clickhouse_tech_user_password")

    s3_key = context["ti"].xcom_pull(
        task_ids="upload_to_s3", key="s3_key"
    )

    s3 = boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
    )
    tmp_path = f"/tmp/{MY_LOGIN}_batch.json"
    s3.download_file(S3_BUCKET, s3_key, tmp_path)

    with open(tmp_path, "rb") as f:
        raw = f.read()

    with Client(
        host=ch_host,
        user=ch_user,
        password=ch_pass,
        database=f"{MY_LOGIN}_db",
        port=9000,
    ) as client:
        # 1) сначала грузим «сырой» стейджинг-слой (как есть из RabbitMQ)
        client.execute(
            "INSERT INTO fact_orders_raw FORMAT JSONEachRow",
            raw,
        )
        # 2) строим витрину из стейджинга (или просто переливаем)
        client.execute(
            """
            INSERT INTO fact_orders
            SELECT
                event_id                                AS order_id,
                toDate(parseDateTimeBestEffort(event_time)) AS event_date,
                customer_id,
                receipt_status,
                store_city,
                store_region,
                product_id,
                product_category,
                product_brand,
                toUInt32(quantity)                       AS quantity,
                toDecimal64(gross_amount, 2)             AS gross_amount,
                toDecimal64(discount_amount, 2)          AS discount_amount,
                toDecimal64(net_amount, 2)               AS net_amount
            FROM fact_orders_raw
            WHERE event_id NOT IN (SELECT order_id FROM fact_orders)
            """
        )