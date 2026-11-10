import json
import logging
from datetime import datetime, timezone

log = logging.getLogger(__name__)

LOGIN = 'evdokimenko_ag'
CH_DB = f'{LOGIN}_db'

RABBIT_HOST = 'rabbitmq.student.magnit.ru'
RABBIT_PORT = 5672
RABBIT_VHOST = '/'
QUEUE = 'supply.queue'
MAX_MESSAGES = 100
WAIT_SECONDS = 15

S3_ENDPOINT = 'https://storage.yandexcloud.net'
S3_BUCKET = 'dad-ksu-datalab'
S3_PREFIX = f'DATA/{LOGIN}/supply'
S3_NAMED_COLLECTION = 's3_dad_ksu_datalab'

CH_URL = 'http://111.88.243.32:8123'

CH_LOGIN_VAR = 'clickhouse_svc_login'
CH_PASSWORD_VAR = 'clickhouse_svc_password'

S3_FILE_STRUCTURE = (
    'event_id String, event_time DateTime, event_date Date, '
    'supply_id String, supply_line_id String, '
    'supplier_id String, supplier_name String, supplier_country String, '
    'product_id String, product_name String, product_category String, product_brand String, '
    'warehouse_id String, warehouse_name String, warehouse_region String, '
    'ordered_qty Nullable(Decimal(18, 3)), received_qty Nullable(Decimal(18, 3)), '
    'rejected_qty Nullable(Decimal(18, 3)), received_amount Nullable(Decimal(18, 2)), '
    'delay_days Nullable(Int32)'
)


def _var(name):
    from airflow.models import Variable
    return Variable.get(name)


def _s3_client():
    import boto3
    import urllib3
    urllib3.disable_warnings()
    session = boto3.session.Session()
    return session.client(
        service_name='s3',
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id=_var('s3_dad_ksu_dataplatform_access_key'),
        aws_secret_access_key=_var('s3_dad_ksu_dataplatform_secret_key'),
        verify=False,
    )


def _s(value):
    return '' if value is None else str(value).strip()


def _num(value):
    if value is None or value == '':
        return None
    return float(value)


def _int(value):
    if value is None or value == '':
        return None
    return int(float(value))


def _parse_time(value):
    if not value:
        return datetime.now(timezone.utc)
    dt = datetime.fromisoformat(str(value).replace('Z', '+00:00'))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def parse_message(msg):
    ts = _parse_time(msg.get('event_time'))
    return {
        'event_id': _s(msg.get('event_id')),
        'event_time': ts.strftime('%Y-%m-%d %H:%M:%S'),
        'event_date': ts.strftime('%Y-%m-%d'),
        'supply_id': _s(msg.get('supply_id')),
        'supply_line_id': _s(msg.get('supply_line_id')),
        'supplier_id': _s(msg.get('supplier_id')),
        'supplier_name': _s(msg.get('supplier_name')),
        'supplier_country': _s(msg.get('supplier_country')),
        'product_id': _s(msg.get('product_id')),
        'product_name': _s(msg.get('product_name')),
        'product_category': _s(msg.get('product_category')),
        'product_brand': _s(msg.get('product_brand')),
        'warehouse_id': _s(msg.get('warehouse_id')),
        'warehouse_name': _s(msg.get('warehouse_name')),
        'warehouse_region': _s(msg.get('warehouse_region')),
        'ordered_qty': _num(msg.get('ordered_qty')),
        'received_qty': _num(msg.get('received_qty')),
        'rejected_qty': _num(msg.get('rejected_qty')),
        'received_amount': _num(msg.get('received_amount')),
        'delay_days': _int(msg.get('delay_days')),
    }


def _collect_messages():
    import pika

    credentials = pika.PlainCredentials(_var('rabbit_mq_svc_login'), _var('rabbit_mq_svc_password'))
    params = pika.ConnectionParameters(
        host=RABBIT_HOST, port=RABBIT_PORT, virtual_host=RABBIT_VHOST,
        credentials=credentials, heartbeat=600, blocked_connection_timeout=300,
    )
    connection = pika.BlockingConnection(params)
    channel = connection.channel()
    channel.basic_qos(prefetch_count=MAX_MESSAGES)

    collected, seen, last_tag = [], set(), None
    try:
        for method, _props, body in channel.consume(
            QUEUE, auto_ack=False, inactivity_timeout=WAIT_SECONDS
        ):
            if method is None:
                break
            last_tag = method.delivery_tag
            try:
                msg = json.loads(body.decode('utf-8'))
            except (json.JSONDecodeError, UnicodeDecodeError):
                log.warning('Пропущено сообщение — не JSON')
                continue
            uid = msg.get('event_id')
            if uid in seen:
                continue
            seen.add(uid)
            collected.append(msg)
            if len(collected) >= MAX_MESSAGES:
                break
    finally:
        if last_tag is not None:
            try:
                channel.basic_nack(delivery_tag=last_tag, multiple=True, requeue=True)
            except Exception as exc:
                log.warning('basic_nack не прошёл: %s', exc)
        connection.close()
    return collected


def rabbit_to_s3(**context):
    messages = _collect_messages()
    log.info('Получено сообщений из %s: %s', QUEUE, len(messages))
    if not messages:
        from airflow.exceptions import AirflowSkipException
        raise AirflowSkipException('Очередь пуста — в S3 писать нечего')

    rows, bad = [], 0
    for msg in messages:
        try:
            row = parse_message(msg)
        except (ValueError, TypeError) as exc:
            bad += 1
            log.warning('Не разобрано сообщение %s: %s', msg.get('event_id'), exc)
            continue
        if row['event_id'] and row['supplier_id'] and row['product_id'] and row['warehouse_id']:
            rows.append(row)
        else:
            bad += 1
    log.info('Разобрано строк: %s, отброшено: %s', len(rows), bad)
    if not rows:
        raise ValueError('Ни одно сообщение не прошло разбор')

    run_ts = datetime.now(timezone.utc)
    key = (f"{S3_PREFIX}/load_date={run_ts:%Y-%m-%d}/"
           f"supply_{run_ts:%Y%m%dT%H%M%S}_{context['run_id'].replace(':', '-').replace('+', '-')}.json")
    body = '\n'.join(json.dumps(r, ensure_ascii=False) for r in rows).encode('utf-8')

    _s3_client().put_object(Bucket=S3_BUCKET, Key=key, Body=body)
    log.info('Файл записан: s3://%s/%s (%s строк)', S3_BUCKET, key, len(rows))
    return {'s3_key': key, 'rows': len(rows)}


def _ch(query):
    import requests
    resp = requests.post(
        CH_URL,
        params={'database': CH_DB},
        data=query.encode('utf-8'),
        auth=(_var(CH_LOGIN_VAR), _var(CH_PASSWORD_VAR)),
        timeout=300,
    )
    if resp.status_code != 200:
        raise RuntimeError(f'ClickHouse вернул {resp.status_code}: {resp.text[:1000]}')
    return resp.text


def s3_to_clickhouse(**context):
    info = context['ti'].xcom_pull(task_ids='rabbit_to_s3')
    if not info:
        from airflow.exceptions import AirflowSkipException
        raise AirflowSkipException('Нет нового файла в S3')
    key = info['s3_key']


    src_named = (f"s3({S3_NAMED_COLLECTION}, filename='{S3_BUCKET}/{key}', "
                 f"format='JSONEachRow', structure='{S3_FILE_STRUCTURE}')")

    src_keys = (f"s3('{S3_ENDPOINT}/{S3_BUCKET}/{key}', "
                f"'{_var('s3_dad_ksu_dataplatform_access_key')}', "
                f"'{_var('s3_dad_ksu_dataplatform_secret_key')}', "
                f"'JSONEachRow', '{S3_FILE_STRUCTURE}')")
    try:
        _ch(f"SELECT count() FROM {src_named}")
        src = src_named
        log.info('Читаем файл через именованную коллекцию %s', S3_NAMED_COLLECTION)
    except RuntimeError as exc:
        log.warning('Именованная коллекция недоступна (%s) — читаем по ключам', str(exc)[:200])
        src = src_keys

    steps = {
        'dim_date': f"""
            INSERT INTO {CH_DB}.dim_date (date_key, full_date, year, month, day, day_of_week)
            SELECT DISTINCT toUInt32(toYYYYMMDD(event_date)), event_date,
                   toYear(event_date), toMonth(event_date),
                   toDayOfMonth(event_date), toDayOfWeek(event_date)
            FROM {src}""",
        'dim_supplier': f"""
            INSERT INTO {CH_DB}.dim_supplier (supplier_id, supplier_name, supplier_country)
            SELECT supplier_id, any(supplier_name), any(supplier_country)
            FROM {src} GROUP BY supplier_id""",
        'dim_product': f"""
            INSERT INTO {CH_DB}.dim_product (product_id, product_name, product_category, product_brand)
            SELECT product_id, any(product_name), any(product_category), any(product_brand)
            FROM {src} GROUP BY product_id""",
        'dim_warehouse': f"""
            INSERT INTO {CH_DB}.dim_warehouse (warehouse_id, warehouse_name, warehouse_region)
            SELECT warehouse_id, any(warehouse_name), any(warehouse_region)
            FROM {src} GROUP BY warehouse_id""",
        'fact_supply_line': f"""
            INSERT INTO {CH_DB}.fact_supply_line
                (event_id, event_time, date_key, supply_id, supply_line_id,
                 supplier_id, product_id, warehouse_id,
                 ordered_qty, received_qty, rejected_qty, received_amount, delay_days, s3_file)
            SELECT event_id, event_time, toUInt32(toYYYYMMDD(event_date)), supply_id, supply_line_id,
                   supplier_id, product_id, warehouse_id,
                   ordered_qty, received_qty, rejected_qty, received_amount, delay_days,
                   '{key}'
            FROM {src}""",
    }
    for table, sql in steps.items():
        _ch(sql)
        log.info('Загружено в %s.%s', CH_DB, table)

    total = _ch(f"SELECT count() FROM {CH_DB}.fact_supply_line FINAL").strip()
    log.info('Файл %s загружен (%s строк). Всего в fact_supply_line: %s', key, info['rows'], total)
