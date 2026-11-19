import json
import logging
from datetime import datetime, timezone

log = logging.getLogger(__name__)

LOGIN = 'evdokimenko_ag'
CH_DB = f'{LOGIN}_db'

RABBIT_HOST_VAR = 'rabbit_mq_host'
RABBIT_PORT = 5672
RABBIT_VHOST = '/'
QUEUE = 'supply.queue'
MAX_MESSAGES = 100
WAIT_SECONDS = 15

S3_ENDPOINT = 'https://storage.yandexcloud.net'
S3_BUCKET = 'dad-ksu-datalab'
S3_PREFIX = f'DATA/{LOGIN}/supply'
S3_NAMED_COLLECTION = 's3_dad_ksu_datalab'

CH_HOST_VAR = 'clickhouse_host'
CH_PORT = 9000

CH_LOGIN_VAR = 'clickhouse_tech_user_account'
CH_PASSWORD_VAR = 'clickhouse_tech_user_password'

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
        host=_var(RABBIT_HOST_VAR), port=RABBIT_PORT, virtual_host=RABBIT_VHOST,
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
    key = (f"{S3_PREFIX}/{run_ts:%Y-%m-%d}/"
           f"supply_{run_ts:%Y%m%dT%H%M%S}_{context['run_id'].replace(':', '-').replace('+', '-')}.json")
    body = '\n'.join(json.dumps(r, ensure_ascii=False) for r in rows).encode('utf-8')

    _s3_client().put_object(Bucket=S3_BUCKET, Key=key, Body=body)
    log.info('Файл записан: s3://%s/%s (%s строк)', S3_BUCKET, key, len(rows))
    return {'s3_key': key, 'rows': len(rows)}


def _ch(query):
    from clickhouse_driver.client import Client
    with Client(
        host=_var(CH_HOST_VAR),
        user=_var(CH_LOGIN_VAR),
        password=_var(CH_PASSWORD_VAR),
        database=CH_DB,
        port=CH_PORT,
    ) as client:
        return client.execute(query)


DIMS = {
    'dim_date': {
        'key': 'date_key',
        'columns': 'date_key, full_date, year, month, day, day_of_week',
        'new_rows': """
            SELECT DISTINCT toUInt32(toYYYYMMDD(event_date)) AS date_key, event_date AS full_date,
                   toUInt16(toYear(event_date)) AS year, toUInt8(toMonth(event_date)) AS month,
                   toUInt8(toDayOfMonth(event_date)) AS day, toUInt8(toDayOfWeek(event_date)) AS day_of_week
            FROM {src}""",
    },
    'dim_supplier': {
        'key': 'supplier_id',
        'columns': 'supplier_id, supplier_name, supplier_country',
        'new_rows': """
            SELECT supplier_id, any(supplier_name) AS supplier_name, any(supplier_country) AS supplier_country
            FROM {src}
            GROUP BY supplier_id""",
    },
    'dim_product': {
        'key': 'product_id',
        'columns': 'product_id, product_name, product_category, product_brand',
        'new_rows': """
            SELECT product_id, any(product_name) AS product_name,
                   any(product_category) AS product_category, any(product_brand) AS product_brand
            FROM {src}
            GROUP BY product_id""",
    },
    'dim_warehouse': {
        'key': 'warehouse_id',
        'columns': 'warehouse_id, warehouse_name, warehouse_region',
        'new_rows': """
            SELECT warehouse_id, any(warehouse_name) AS warehouse_name, any(warehouse_region) AS warehouse_region
            FROM {src}
            GROUP BY warehouse_id""",
    },
}


def _s3_key(context):
    info = context['ti'].xcom_pull(task_ids='rabbit_to_s3')
    if not info:
        from airflow.exceptions import AirflowSkipException
        raise AirflowSkipException('Нет нового файла в S3')
    return info['s3_key']


def _s3_source(key):
    return (f"s3({S3_NAMED_COLLECTION}, filename='{S3_BUCKET}/{key}', "
            f"format='JSONEachRow', structure='{S3_FILE_STRUCTURE}')")


def s3_to_stage(**context):
    key = _s3_key(context)
    src = _s3_source(key)
    for dim, cfg in DIMS.items():
        stage = f'{CH_DB}.{dim}_stage'
        prod = f'{CH_DB}.{dim}'
        new_rows = cfg['new_rows'].format(src=src)
        _ch(f'TRUNCATE TABLE {stage}')
        _ch(f"""
            INSERT INTO {stage} ({cfg['columns']})
            SELECT {cfg['columns']} FROM ({new_rows})""")
        _ch(f"""
            INSERT INTO {stage} ({cfg['columns']})
            SELECT {cfg['columns']} FROM {prod}
            WHERE {cfg['key']} NOT IN (SELECT {cfg['key']} FROM {stage})""")
        rows = _ch(f'SELECT count() FROM {stage}')[0][0]
        log.info('%s: в stage загружено %s строк (старые + новые из %s)', stage, rows, key)


def swap_stage_prod(**context):
    for dim in DIMS:
        _ch(f'EXCHANGE TABLES {CH_DB}.{dim}_stage AND {CH_DB}.{dim}')
        log.info('Подмена выполнена: %s.%s_stage <-> %s.%s', CH_DB, dim, CH_DB, dim)


def clear_stage(**context):
    for dim in DIMS:
        _ch(f'TRUNCATE TABLE {CH_DB}.{dim}_stage')
        log.info('Очищена %s.%s_stage', CH_DB, dim)


def load_fact(**context):
    key = _s3_key(context)
    src = _s3_source(key)
    before = _ch(f'SELECT count() FROM {CH_DB}.fact_supply_line')[0][0]
    _ch(f"""
        INSERT INTO {CH_DB}.fact_supply_line
            (event_id, event_time, date_key, supply_id, supply_line_id,
             supplier_id, product_id, warehouse_id,
             ordered_qty, received_qty, rejected_qty, received_amount, delay_days, s3_file)
        SELECT event_id, event_time, toUInt32(toYYYYMMDD(event_date)), supply_id, supply_line_id,
               supplier_id, product_id, warehouse_id,
               ordered_qty, received_qty, rejected_qty, received_amount, delay_days,
               '{key}'
        FROM {src}
        WHERE event_id NOT IN (SELECT event_id FROM {CH_DB}.fact_supply_line)""")
    after = _ch(f'SELECT count() FROM {CH_DB}.fact_supply_line')[0][0]
    log.info('fact_supply_line: добавлено %s новых строк, всего %s', after - before, after)


def check_duplicates(**context):
    tables = {f'{CH_DB}.{dim}': cfg['key'] for dim, cfg in DIMS.items()}
    tables[f'{CH_DB}.fact_supply_line'] = 'event_id'
    found = False
    for table, key in tables.items():
        total, unique = _ch(f'SELECT count(), uniqExact({key}) FROM {table}')[0]
        dups = total - unique
        if dups:
            found = True
            log.warning('ДУБЛИКАТЫ: в %s %s повторов по ключу %s (строк %s, уникальных %s)',
                        table, dups, key, total, unique)
        else:
            log.info('Дубликатов нет: %s — %s строк, все %s уникальны', table, total, key)
    if found:
        log.warning('Проверка завершена: найдены дубликаты')
    else:
        log.info('Проверка завершена: дубликатов нет ни в одной таблице')
