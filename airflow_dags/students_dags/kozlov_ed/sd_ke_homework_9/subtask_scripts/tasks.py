import json
import re
from datetime import datetime, timezone
from decimal import Decimal

LOGIN = "kozlov_ed"
DATABASE = LOGIN + "_db"
BUCKET = "dad-ksu-datalab"
STRUCTURE = (
    "event_id String, event_time DateTime64(6, 'UTC'), order_key String, "
    "product_key UInt64, customer_key UInt64, date_key UInt32, date Date, "
    "month UInt8, year UInt16, status String, product_name String, "
    "category String, brand String, segment String, quantity UInt32, "
    "gross_amount Decimal(12,2), discount_amount Decimal(12,2), net_amount Decimal(12,2)"
)

TABLES = {
    "dim_date": ("date_key UInt32, date Date, month UInt8, year UInt16", "date_key"),
    "dim_product": ("product_key UInt64, product_id UInt64, product_name String, category String, brand String, event_time DateTime64(6, 'UTC')", "product_key, event_time"),
    "dim_customer": ("customer_key UInt64, customer_id UInt64, segment String, event_time DateTime64(6, 'UTC')", "customer_key, event_time"),
    "dim_order": ("order_key String, order_id String, status String, event_time DateTime64(6, 'UTC')", "order_key, event_time"),
    "fact_order_items": ("event_id String, order_key String, product_key UInt64, date_key UInt32, customer_key UInt64, quantity UInt32, gross_amount Decimal(12,2), discount_amount Decimal(12,2), net_amount Decimal(12,2)", "event_id"),
}

def ddl():
    return [f"CREATE TABLE IF NOT EXISTS {DATABASE}.{name} ({columns}) ENGINE=MergeTree ORDER BY ({key})"
            for name, (columns, key) in TABLES.items()]

def normalize(body):
    m = json.loads(body, parse_float=Decimal)
    if not isinstance(m, dict) or m.get("event_type") != "order_item":
        raise ValueError("Ожидается JSON-объект order_item")
    for field in ("event_id", "event_time", "order_id", "order_status", "product_id", "customer_id", "quantity", "gross_amount", "discount_amount", "net_amount"):
        if m.get(field) is None or m[field] == "":
            raise ValueError(f"Отсутствует поле {field}")
    if not all(isinstance(m[f], str) for f in ("event_id", "order_id", "order_status")):
        raise ValueError("event_id, order_id и order_status должны быть строками")
    stamp = datetime.fromisoformat(m["event_time"].replace("Z", "+00:00"))
    if stamp.tzinfo is None:
        raise ValueError("event_time должен содержать часовой пояс")
    stamp = stamp.astimezone(timezone.utc)
    if not 1970 <= stamp.year <= 2149:
        raise ValueError("Дата вне поддерживаемого диапазона Date")
    def identifier(field, prefix):
        value = m[field]
        if not isinstance(value, str) or not re.fullmatch(prefix + r"-[0-9]+", value):
            raise ValueError(f"Неверный {field}")
        result = int(value.split("-", 1)[1])
        if result > 2**64 - 1:
            raise ValueError(f"{field} вне UInt64")
        return result
    quantity = Decimal(str(m["quantity"]))
    if not quantity.is_finite() or quantity != quantity.to_integral_value() or not 0 < quantity <= 2**32 - 1:
        raise ValueError("quantity должен быть положительным UInt32")
    amounts = {}
    for field in ("gross_amount", "discount_amount", "net_amount"):
        value = Decimal(str(m[field]))
        if not value.is_finite() or abs(value) >= Decimal("10000000000") or value != value.quantize(Decimal("0.01")):
            raise ValueError(f"{field} не помещается в Decimal(12,2)")
        amounts[field] = value
    if amounts["net_amount"] != amounts["gross_amount"] - amounts["discount_amount"]:
        raise ValueError("net_amount != gross_amount - discount_amount")
    row = dict(event_id=m["event_id"], event_time=stamp.strftime("%Y-%m-%d %H:%M:%S.%f"),
               order_key=m["order_id"], product_key=identifier("product_id", "PRD"),
               customer_key=identifier("customer_id", "CUST"), date_key=int(stamp.strftime("%Y%m%d")),
               date=stamp.date().isoformat(), month=stamp.month, year=stamp.year,
               status=m["order_status"], quantity=int(quantity))
    for dest, source in (("product_name", "product_name"), ("category", "product_category"),
                         ("brand", "product_brand"), ("segment", "customer_segment")):
        value = m.get(source)
        if value is not None and not isinstance(value, str):
            raise ValueError(f"{source} должен быть строкой")
        row[dest] = value or ""
    row.update({k: format(v, ".2f") for k, v in amounts.items()})
    return row

def load_queries(source):
    projections = {
        "dim_date": "date_key, date, month, year",
        "dim_product": "product_key, product_key AS product_id, product_name, category, brand, event_time",
        "dim_customer": "customer_key, customer_key AS customer_id, segment, event_time",
        "dim_order": "order_key, order_key AS order_id, status, event_time",
        "fact_order_items": "event_id, order_key, product_key, date_key, customer_key, quantity, gross_amount, discount_amount, net_amount",
    }
    result = []
    for name, projection in projections.items():
        key = TABLES[name][1]

        target = ", ".join(p.strip().split(" AS ")[-1] for p in projection.split(","))
        result.append(f"INSERT INTO {DATABASE}.{name} ({target}) SELECT DISTINCT {projection} FROM {source} WHERE ({key}) NOT IN (SELECT {key} FROM {DATABASE}.{name})")
    return result


"""Все внешние подключения создаются только при выполнении задач Airflow."""
import hashlib
import json
import logging


def s3_client():
    import boto3
    from airflow.models import Variable
    return boto3.client(
        's3', endpoint_url='https://storage.yandexcloud.net',
        aws_access_key_id=Variable.get('s3_dad_ksu_dataplatform_access_key'),
        aws_secret_access_key=Variable.get('s3_dad_ksu_dataplatform_secret_key'),
    )


def clickhouse_client():
    from airflow.models import Variable
    from clickhouse_driver import Client

    host = Variable.get('clickhouse_host', default_var=None)
    if not host:
        host = Variable.get('clickhouse_port', default_var='111.88.243.32')
    if host.isdigit() or '://' in host or '/' in host:
        raise ValueError('Укажите адрес сервера в Airflow Variable clickhouse_host (без протокола/порта)')
    return Client(host=host, port=9000, database=DATABASE,
                  user=Variable.get('clickhouse_tech_user_account'),
                  password=Variable.get('clickhouse_tech_user_password'),
                  connect_timeout=10, send_receive_timeout=120)


def rabbit_to_s3(**context):

    import pika
    from airflow.models import Variable
    from botocore.exceptions import ClientError
    client = s3_client()
    digest = hashlib.sha256(context['run_id'].encode()).hexdigest()
    key = f'DATA/{LOGIN}/hw9/{digest}.jsonl'

    try:
        metadata = client.head_object(Bucket=BUCKET, Key=key)
    except ClientError as exc:
        if str(exc.response['Error']['Code']) not in ('404', 'NoSuchKey', 'NotFound'):
            raise
    else:
        logging.info('Используем сохранённый пакет текущего запуска')
        return {'key': key, 'rows': int(metadata.get('Metadata', {}).get('rows', '0'))}
    connection = pika.BlockingConnection(pika.ConnectionParameters(
        host=Variable.get('rabbit_mq_host'), port=5672,
        credentials=pika.PlainCredentials(Variable.get('rabbit_mq_svc_login'),
                                          Variable.get('rabbit_mq_svc_password')),
        heartbeat=600, socket_timeout=10, blocked_connection_timeout=30,
    ))
    channel = connection.channel()
    rows, last_tag = {}, None
    try:


        for _ in range(100):
            method, _, body = channel.basic_get(queue='ecom.queue', auto_ack=False)
            if method is None:
                break
            last_tag = method.delivery_tag
            row = normalize(body)
            previous = rows.get(row['event_id'])
            if previous is not None and previous != row:
                raise ValueError('Один event_id содержит разные данные')
            rows[row['event_id']] = row
            connection.process_data_events(time_limit=0)
        payload = ''.join(json.dumps(row, ensure_ascii=False) + '\n' for row in rows.values()).encode('utf-8')
        client.put_object(Bucket=BUCKET, Key=key, Body=payload,
                          ContentType='application/x-ndjson', Metadata={'rows': str(len(rows))})
        logging.info('Сохранено %s уникальных событий в %s', len(rows), key)
        return {'key': key, 'rows': len(rows)}
    finally:
        try:
            if last_tag is not None and channel.is_open:
                channel.basic_nack(delivery_tag=last_tag, multiple=True, requeue=True)
        finally:
            if connection.is_open:
                connection.close()


def s3_to_clickhouse(**context):

    batch = context['ti'].xcom_pull(task_ids='rabbit_to_s3')
    if not isinstance(batch, dict) or not batch.get('key', '').startswith(f'DATA/{LOGIN}/hw9/'):
        raise ValueError('Нет корректного пути S3 в XCom')

    source = (
        "s3(s3_dad_ksu_datalab, filename=%(filename)s, "
        "format='JSONEachRow', structure=%(structure)s)"
    )
    with clickhouse_client() as client:
        for query in ddl():
            client.execute(query)
        if not batch['rows']:
            logging.info('Пакет пуст; таблицы готовы, загрузка пропущена')
            return
        params = {'filename': f"{BUCKET}/{batch['key']}", 'structure': STRUCTURE}


        for query in load_queries(source):
            client.execute(query, params)
        count = client.execute(f'SELECT count(), uniqExact(event_id) FROM {DATABASE}.fact_order_items')[0]
        if count[0] != count[1]:
            raise ValueError('Обнаружены дубли event_id; проверьте параллельные записи')
        logging.info('Загрузка завершена; всего фактов: %s', count[0])
