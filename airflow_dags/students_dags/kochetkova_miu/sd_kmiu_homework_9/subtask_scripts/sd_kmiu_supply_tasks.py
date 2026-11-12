import io
import json
import datetime
import pika
import pandas as pd
import boto3
import urllib3
from clickhouse_driver import Client
from airflow.models import Variable

urllib3.disable_warnings()


def rabbitmq_to_s3():
    rabbit_host = Variable.get('rabbit_mq_host')
    rabbit_user = Variable.get('rabbit_mq_svc_login')
    rabbit_pass = Variable.get('rabbit_mq_svc_password')
    
    access_key = Variable.get('s3_dad_ksu_dataplatform_access_key')
    secret_key = Variable.get('s3_dad_ksu_dataplatform_secret_key')
    
    QUEUE = "sales.queue"
    MAX_MESSAGES = 100
    collected = []

    credentials = pika.PlainCredentials(rabbit_user, rabbit_pass)
    parameters = pika.ConnectionParameters(
        host=rabbit_host,
        port=5672,
        virtual_host="/",
        credentials=credentials
    )
    
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()
    channel.basic_qos(prefetch_count=1)

    def callback(ch, method, properties, body):
        try:
            message = json.loads(body.decode("utf-8"))
        except json.JSONDecodeError:
            message = {"_raw": body.decode("utf-8", errors="replace")}

        collected.append(message)
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)

        if len(collected) >= MAX_MESSAGES:
            ch.stop_consuming()

    channel.basic_consume(queue=QUEUE, on_message_callback=callback, auto_ack=False)
    
    try:
        channel.start_consuming()
    finally:
        connection.close()

    fact_rows = []
    date_rows = {}
    product_rows = {}
    warehouse_rows = {}
    supplier_rows = {
        1: {
            'supplier_key': 1,
            'supplier_id': 1,
            'supplier_name': 'Основной поставщик',
            'country': 'Россия'
        }
    }

    for message in collected:
        if "_raw" in message:
            continue
            
        sale_date = datetime.datetime.strptime(message["event_time"].split("T")[0], "%Y-%m-%d").date()
        date_key = int(sale_date.strftime("%Y%m%d"))
        
        product_key = int(message["product_id"].replace("PRD-", ""))
        warehouse_key = int(message["store_id"].replace("ST-", ""))
        supply_key = int(message["receipt_id"].replace("R-", ""))
        supplier_key = 1 

        quantity = int(message["quantity"])
        cost_amount = int(float(message["cost_amount"]))
        net_amount = float(message["net_amount"])
        unit_purchase_price = int(net_amount / quantity) if quantity > 0 else 0
        rejected_quantity = 0

        date_rows[date_key] = {
            'date_key': date_key,
            'date': str(sale_date),
            'day': sale_date.day,
            'month': sale_date.month,
            'quarter': (sale_date.month - 1) // 3 + 1,
            'year': sale_date.year,
            'week_day': sale_date.strftime('%A')
        }

        product_rows[product_key] = {
            'product_key': product_key,
            'product_id': product_key,
            'product_name': str(message['product_name']),
            'brand': str(message['product_brand']),
            'category': str(message['product_category'])
        }

        warehouse_rows[warehouse_key] = {
            'warehouse_key': warehouse_key,
            'warehouse_id': warehouse_key,
            'warehouse_name': str(message['store_name']),
            'city': str(message['store_city'])
        }

        fact_rows.append({
            'supply_key': supply_key,
            'date_key': date_key,
            'product_key': product_key,
            'supplier_key': supplier_key,
            'warehouse_key': warehouse_key,
            'supply_quantity': quantity,
            'purchase_cost': cost_amount,
            'unit_purchase_price': unit_purchase_price,
            'rejected_quantity': rejected_quantity
        })

    session = boto3.session.Session()
    s3_client = session.client(
        service_name='s3',
        endpoint_url='https://storage.yandexcloud.net',
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        verify=False
    )

    tables_to_upload = {
        'fact_supplies': pd.DataFrame(fact_rows),
        'dim_date': pd.DataFrame(list(date_rows.values())),
        'dim_product': pd.DataFrame(list(product_rows.values())),
        'dim_warehouse': pd.DataFrame(list(warehouse_rows.values())),
        'dim_supplier': pd.DataFrame(list(supplier_rows.values()))
    }

    for table_name, df in tables_to_upload.items():
        parquet_buffer = io.BytesIO()
        df.to_parquet(parquet_buffer, index=False)
        parquet_buffer.seek(0)
        s3_client.put_object(
            Bucket='dad-ksu-datalab',
            Key=f'DATA/kochetkova_miu/{table_name}.parquet',
            Body=parquet_buffer.getvalue()
        )
        print(f"Выгружено в S3: {table_name} ({len(df)} строк)")


def s3_to_clickhouse():
    access_key = Variable.get('s3_dad_ksu_dataplatform_access_key')
    secret_key = Variable.get('s3_dad_ksu_dataplatform_secret_key')
    
    clickhouse_host = Variable.get('clickhouse_host')
    clickhouse_svc_login = Variable.get('clickhouse_tech_user_account')
    clickhouse_svc_password = Variable.get('clickhouse_tech_user_password')

    session = boto3.session.Session()
    s3_client = session.client(
        service_name='s3',
        endpoint_url='https://storage.yandexcloud.net',
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        verify=False
    )

    ddl_queries = [
        """
        CREATE TABLE IF NOT EXISTS kochetkova_miu_db.dim_date (
            date_key UInt32,
            date String,
            day UInt8,
            month UInt8,
            quarter UInt8,
            year UInt16,
            week_day String
        ) ENGINE = MergeTree() ORDER BY date_key;
        """,
        """
        CREATE TABLE IF NOT EXISTS kochetkova_miu_db.dim_product (
            product_key UInt64,
            product_id UInt64,
            product_name String,
            brand String,
            category String
        ) ENGINE = MergeTree() ORDER BY product_key;
        """,
        """
        CREATE TABLE IF NOT EXISTS kochetkova_miu_db.dim_warehouse (
            warehouse_key UInt64,
            warehouse_id UInt64,
            warehouse_name String,
            city String
        ) ENGINE = MergeTree() ORDER BY warehouse_key;
        """,
        """
        CREATE TABLE IF NOT EXISTS kochetkova_miu_db.dim_supplier (
            supplier_key UInt32,
            supplier_id UInt32,
            supplier_name String,
            country String
        ) ENGINE = MergeTree() ORDER BY supplier_key;
        """,
        """
        CREATE TABLE IF NOT EXISTS kochetkova_miu_db.fact_supplies (
            supply_key UInt64,
            date_key UInt32,
            product_key UInt64,
            supplier_key UInt32,
            warehouse_key UInt64,
            supply_quantity UInt32,
            purchase_cost UInt64,
            unit_purchase_price UInt32,
            rejected_quantity UInt32
        ) ENGINE = MergeTree() ORDER BY supply_key;
        """
    ]

    with Client(
        host=clickhouse_host, 
        user=clickhouse_svc_login, 
        password=clickhouse_svc_password, 
        database='kochetkova_miu_db',
        settings={'use_numpy': True},
        port=9000
    ) as client:
        
        for query in ddl_queries:
            client.execute(query)

        tables = ['dim_date', 'dim_product', 'dim_warehouse', 'dim_supplier', 'fact_supplies']
        for table_name in tables:
            response = s3_client.get_object(Bucket='dad-ksu-datalab', Key=f'DATA/kochetkova_miu/{table_name}.parquet')
            df = pd.read_parquet(io.BytesIO(response['Body'].read()))
            
            client.execute(f"TRUNCATE TABLE IF EXISTS kochetkova_miu_db.{table_name};")
            client.insert_dataframe(f"INSERT INTO kochetkova_miu_db.{table_name} VALUES", df)
            print(f"Загружено в ClickHouse: {table_name} ({len(df)} строк)")
