import io
import json

import boto3
import pandas as pd
import pika
import urllib3
from airflow.models import Variable
from clickhouse_driver.client import Client

urllib3.disable_warnings()

QUEUE = 'supply.queue'
MAX_MESSAGES = 200

ENDPOINT_URL = 'https://storage.yandexcloud.net'
BUCKET = 'dad-ksu-datalab'
FILE_KEY = 'DATA/zlatkin_va/supply_lines.parquet'


def rabbit_to_s3():
    rabbit_login = Variable.get('rabbit_mq_svc_login')
    rabbit_password = Variable.get('rabbit_mq_svc_password')
    rabbit_host = Variable.get('rabbit_mq_host')

    credentials = pika.PlainCredentials(rabbit_login, rabbit_password)
    parameters = pika.ConnectionParameters(
        host=rabbit_host,
        port=5672,
        virtual_host='/',
        credentials=credentials,
        heartbeat=600,
        blocked_connection_timeout=300,
    )

    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()
    channel.basic_qos(prefetch_count=MAX_MESSAGES)

    collected = []
    last_tag = None

    for method, properties, body in channel.consume(QUEUE, auto_ack=False, inactivity_timeout=30):
        if method is None:
            break
        collected.append(json.loads(body.decode('utf-8')))
        last_tag = method.delivery_tag
        if len(collected) >= MAX_MESSAGES:
            break

    if last_tag is not None:
        channel.basic_nack(delivery_tag=last_tag, multiple=True, requeue=True)
    channel.cancel()
    connection.close()

    print('Прочитано сообщений:', len(collected))
    if len(collected) == 0:
        raise Exception('Очередь пустая, писать в S3 нечего')

    rows = []
    for message in collected:
        ordered_qty = message['ordered_qty']
        received_qty = message['received_qty']
        received_amount = float(message['received_amount'])
        shortage_qty = ordered_qty - received_qty
        if received_qty > 0:
            unit_price = round(received_amount / received_qty, 2)
        else:
            unit_price = None

        rows.append({
            'supply_id': int(message['supply_id'].split('-')[1]),
            'supplier_id': int(message['supplier_id'].split('-')[1]),
            'supplier_name': message['supplier_name'],
            'supplier_country': message['supplier_country'],
            'product_id': int(message['product_id'].split('-')[1]),
            'product_name': message['product_name'],
            'product_category': message['product_category'],
            'product_brand': message['product_brand'],
            'warehouse_id': int(message['warehouse_id'].split('-')[1]),
            'warehouse_name': message['warehouse_name'],
            'warehouse_region': message['warehouse_region'],
            'event_date': message['event_time'][:10],
            'ordered_qty': ordered_qty,
            'received_qty': received_qty,
            'shortage_qty': shortage_qty,
            'received_amount': received_amount,
            'unit_price': unit_price,
            'delay_days': message['delay_days'],
        })

    df = pd.DataFrame(rows)
    df['unit_price'] = df['unit_price'].astype(float)

    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False)

    session = boto3.session.Session()
    s3 = session.client(
        service_name='s3',
        endpoint_url=ENDPOINT_URL,
        aws_access_key_id=Variable.get('s3_dad_ksu_dataplatform_access_key'),
        aws_secret_access_key=Variable.get('s3_dad_ksu_dataplatform_secret_key'),
        verify=False
    )
    s3.put_object(Bucket=BUCKET, Key=FILE_KEY, Body=buffer.getvalue())
    print('Файл записан:', FILE_KEY)


def s3_to_clickhouse():
    access_key = Variable.get('s3_dad_ksu_dataplatform_access_key')
    secret_key = Variable.get('s3_dad_ksu_dataplatform_secret_key')
    clickhouse_svc_login = Variable.get('clickhouse_tech_user_account')
    clickhouse_svc_password = Variable.get('clickhouse_tech_user_password')
    clickhouse_host = Variable.get('clickhouse_port')

    source = f"s3('{ENDPOINT_URL}/{BUCKET}/{FILE_KEY}', '{access_key}', '{secret_key}', 'Parquet')"

    with Client(
        host=clickhouse_host,
        user=clickhouse_svc_login,
        password=clickhouse_svc_password,
        database='zlatkin_va_db',
        port=9000
    ) as client:

        client.execute(f'''
            insert into dim_date (date_key, date)
            select distinct toYYYYMMDD(toDate(event_date)), toDate(event_date)
            from {source}
        ''')

        client.execute(f'''
            insert into dim_supplies (supplier_key, supplier_id, name, region)
            select distinct supplier_id, supplier_id, supplier_name, supplier_country
            from {source}
        ''')

        client.execute(f'''
            insert into dim_product (product_key, product_id, name, category, brand)
            select distinct product_id, product_id, product_name, product_category, product_brand
            from {source}
        ''')

        client.execute(f'''
            insert into dim_warehouse (warehouse_key, warehouse_id, name, city)
            select distinct warehouse_id, warehouse_id, warehouse_name, warehouse_region
            from {source}
        ''')

        client.execute(f'''
            insert into fact_supply_lines
                (date_key, supplier_key, warehouse_key, product_key, supply_id,
                 ordered_qty, received_qty, shortage_qty, received_amount, unit_price, delay_days)
            select
                toYYYYMMDD(toDate(event_date)), supplier_id, warehouse_id, product_id, supply_id,
                ordered_qty, received_qty, shortage_qty, received_amount, unit_price, delay_days
            from {source}
        ''')

        for table in ['dim_date', 'dim_supplies', 'dim_product', 'dim_warehouse', 'fact_supply_lines']:
            count = client.execute(f'select count(*) from {table} final')
            print(table, count[0][0])