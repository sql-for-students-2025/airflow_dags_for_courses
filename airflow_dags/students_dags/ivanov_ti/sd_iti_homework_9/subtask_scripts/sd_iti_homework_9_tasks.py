import datetime
import json

import boto3
import pika
import urllib3
from airflow.models import Variable
from clickhouse_driver.client import Client

urllib3.disable_warnings()

QUEUE = "stock.queue"
# Сколько сообщений забрать. None — всю очередь
MAX_MESSAGES = 100

S3_ENDPOINT = "https://storage.yandexcloud.net"
S3_BUCKET = "dad-ksu-datalab"
S3_FOLDER = "DATA/ivanov_ti"

CLICKHOUSE_DB = "ivanov_ti_db"

# Поля сообщения и их типы для чтения файла в ClickHouse
S3_STRUCTURE = (
    "snapshot_date Date, "
    "warehouse_id String, "
    "warehouse_region String, "
    "product_id String, "
    "product_name String, "
    "product_category String, "
    "product_brand String, "
    "stock_qty Decimal(14, 3), "
    "reserved_qty Decimal(14, 3), "
    "stock_value Decimal(16, 2)"
)


def rabbit_to_s3():
    """Забирает сообщения из RabbitMQ и пишет их файлом в S3."""
    access_key = Variable.get('s3_dad_ksu_dataplatform_access_key')
    secret_key = Variable.get('s3_dad_ksu_dataplatform_secret_key')
    rabbitmq_svc_password = Variable.get('rabbit_mq_svc_password')
    rabbit_mq_svc_login = Variable.get('rabbit_mq_svc_login')
    rabbit_mq_host = Variable.get('rabbit_mq_host')

    credentials = pika.PlainCredentials(rabbit_mq_svc_login, rabbitmq_svc_password)
    parameters = pika.ConnectionParameters(
        host=rabbit_mq_host,
        port=5672,
        virtual_host="/",
        credentials=credentials,
    )

    connection = pika.BlockingConnection(parameters)
    try:
        channel = connection.channel()

        # passive=True только узнаёт размер очереди и ничего в ней не меняет
        total = channel.queue_declare(queue=QUEUE, passive=True).method.message_count
        if MAX_MESSAGES is not None:
            total = min(total, MAX_MESSAGES)
        print(f"Будет прочитано сообщений: {total}")

        collected = []
        if total > 0:
            # Держим все сообщения неподтверждёнными: после закрытия
            # подключения они вернутся в очередь преподавателя
            channel.basic_qos(prefetch_count=min(total, 65535))
            for method, properties, body in channel.consume(
                queue=QUEUE, auto_ack=False, inactivity_timeout=10
            ):
                if method is None:
                    break
                collected.append(json.loads(body.decode("utf-8")))
                if len(collected) >= total:
                    break
            channel.cancel()
    finally:
        connection.close()

    print(f"Собрано сообщений: {len(collected)}")
    if not collected:
        return None

    # JSONEachRow: одно сообщение — одна строка JSON
    data = "\n".join(json.dumps(m, ensure_ascii=False) for m in collected)

    file_time = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    key = f"{S3_FOLDER}/stock_{file_time}.json"

    s3 = boto3.session.Session().client(
        service_name="s3",
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        verify=False,
    )
    s3.put_object(Bucket=S3_BUCKET, Key=key, Body=data.encode("utf-8"))
    print(f"Файл записан: {S3_BUCKET}/{key}")

    # Имя файла уйдёт в XCom и достанется следующей задаче
    return key


def s3_to_clickhouse(ti):
    """Заливает файл из S3 в таблицы ClickHouse."""
    access_key = Variable.get('s3_dad_ksu_dataplatform_access_key')
    secret_key = Variable.get('s3_dad_ksu_dataplatform_secret_key')
    clickhouse_svc_login = Variable.get('clickhouse_tech_user_account')
    clickhouse_svc_password = Variable.get('clickhouse_tech_user_password')
    clickhouse_host = Variable.get('clickhouse_host')

    key = ti.xcom_pull(task_ids="rabbit_to_s3")
    if not key:
        print("Новых данных нет, загружать нечего")
        return

    source = (
        "s3(%(url)s, %(access)s, %(secret)s, 'JSONEachRow', %(structure)s)"
    )
    params = {
        "url": f"{S3_ENDPOINT}/{S3_BUCKET}/{key}",
        "access": access_key,
        "secret": secret_key,
        "structure": S3_STRUCTURE,
    }

    queries = {
        "dim_date": f"""
            insert into {CLICKHOUSE_DB}.dim_date_change (date_key, full_date)
            select distinct toYYYYMMDD(snapshot_date), snapshot_date
            from {source}
        """,
        "dim_product": f"""
            insert into {CLICKHOUSE_DB}.dim_product_change
                (sku, product_name, brand, category, subcategory, supplier)
            select
                product_id,
                any(product_name),
                any(product_brand),
                any(product_category),
                '',
                ''
            from {source}
            group by product_id
        """,
        "dim_location": f"""
            insert into {CLICKHOUSE_DB}.dim_location_change
                (location_code, location_type, city, region)
            select
                warehouse_id,
                'warehouse',
                any(warehouse_region),
                any(warehouse_region)
            from {source}
            group by warehouse_id
        """,
        "fact_inventory_daily": f"""
            insert into {CLICKHOUSE_DB}.fact_inventory_daily (
                date_key, sku, location_code,
                qty_on_hand, qty_reserved, qty_available,
                stock_value_cost, is_out_of_stock
            )
            select
                toYYYYMMDD(snapshot_date),
                product_id,
                warehouse_id,
                stock_qty,
                reserved_qty,
                stock_qty - reserved_qty,
                stock_value,
                if(stock_qty - reserved_qty <= 0, 1, 0)
            from {source}
        """,
    }

    with Client(
        host=clickhouse_host,
        user=clickhouse_svc_login,
        password=clickhouse_svc_password,
        database=CLICKHOUSE_DB,
        port=9000,
    ) as client:
        for table, sql in queries.items():
            # Лишние поля сообщения (event_time, unit_cost и т.д.) пропускаем
            client.execute(
                sql + " settings input_format_skip_unknown_fields = 1", params
            )
            client.execute(exchange tables dim_date_change and dim_date)
            client.execute(truncate table dim_date_change)
            client.execute(exchange tables dim_product_change and dim_product)
            client.execute(truncate table dim_product_change)
            client.execute(exchange tables dim_location_change and dim_location)
            client.execute(truncate table dim_location_change)
            
            count = client.execute(
                f"select count() from {CLICKHOUSE_DB}.{table} final"
            )[0][0]
            print(f"{table}: {count} строк")
