import hashlib
import json

LOGIN = "nechaev_sv"
QUEUE = "movement.queue"
MAX_MESSAGES = 5000

S3_ENDPOINT = "https://storage.yandexcloud.net"
S3_BUCKET = "dad-ksu-datalab"
S3_PREFIX = f"DATA/{LOGIN}/movement"
S3_COLLECTION = "s3_dad_ksu_datalab"

CH_DATABASE = f"{LOGIN}_db"

STG_COLUMNS = [
    ("event_id", "String"),
    ("event_type", "String"),
    ("event_time", "String"),
    ("movement_id", "String"),
    ("movement_type", "String"),
    ("product_id", "String"),
    ("product_name", "String"),
    ("product_category", "String"),
    ("product_brand", "String"),
    ("from_warehouse_id", "Nullable(String)"),
    ("from_warehouse_name", "Nullable(String)"),
    ("from_warehouse_region", "Nullable(String)"),
    ("to_warehouse_id", "Nullable(String)"),
    ("to_warehouse_name", "Nullable(String)"),
    ("to_warehouse_region", "Nullable(String)"),
    ("quantity", "Int32"),
    ("cost_amount", "Float64"),
    ("retail_amount", "Float64"),
    ("weight_kg", "Float64"),
    ("volume_m3", "Float64"),
]

MOVEMENT_TYPES = [
    (1, "RECEIPT", "receipt", 1, 0),
    (2, "SUPPLIER_RETURN", "supplier_return", -1, 0),
    (3, "CUSTOMER_RETURN", "customer_return", 1, 0),
    (4, "SALE", "sale", -1, 0),
    (5, "SHIPMENT", "shipment", -1, 0),
    (6, "WRITE_OFF", "write_off", -1, 0),
    (7, "TRANSFER_IN", "transfer", 1, 1),
    (8, "TRANSFER_OUT", "transfer", -1, 1),
    (9, "ADJUSTMENT_IN", "adjustment", 1, 0),
    (10, "ADJUSTMENT_OUT", "adjustment", -1, 0),
    (11, "OTHER_IN", "other", 1, 0),
    (12, "OTHER_OUT", "other", -1, 0),
    (13, "RETURN_IN", "return", 1, 1),
    (14, "RETURN_OUT", "return", -1, 1),
]

DDL = [
    "CREATE TABLE IF NOT EXISTS stg_movement ("
    + ", ".join(f"{name} {kind}" for name, kind in STG_COLUMNS)
    + ", source_file String) ENGINE = MergeTree ORDER BY event_id",
    """CREATE TABLE IF NOT EXISTS dim_date (
        date_key UInt32, date Date, day UInt8, week UInt8, month UInt8, quarter UInt8, year UInt16
    ) ENGINE = MergeTree ORDER BY date_key""",
    """CREATE TABLE IF NOT EXISTS dim_product (
        product_key UInt32, product_id String, name String, brand String, category String
    ) ENGINE = MergeTree ORDER BY product_key""",
    """CREATE TABLE IF NOT EXISTS dim_location (
        location_key UInt32, location_id String, type String, name String, city String
    ) ENGINE = MergeTree ORDER BY location_key""",
    """CREATE TABLE IF NOT EXISTS dim_movement_type (
        movement_type_key UInt8, type_code String, movement_group String, sign Int8, is_internal UInt8
    ) ENGINE = MergeTree ORDER BY movement_type_key""",
    """CREATE TABLE IF NOT EXISTS dim_supplier (
        supplier_key UInt32, supplier_id String, name String, inn String, city String
    ) ENGINE = MergeTree ORDER BY supplier_key""",
    """CREATE TABLE IF NOT EXISTS fact_inventory_movement (
        movement_date_key UInt32,
        product_key UInt32,
        location_key UInt32,
        counter_location_key UInt32,
        movement_type_key UInt8,
        supplier_key UInt32,
        document_id String,
        document_line_no UInt32,
        event_id String,
        event_time DateTime64(6, 'UTC'),
        qty_in Decimal(14, 3),
        qty_out Decimal(14, 3),
        net_qty Decimal(14, 3),
        unit_cost Decimal(12, 2),
        net_cost_amount Decimal(16, 2)
    ) ENGINE = MergeTree
    PARTITION BY toYYYYMM(event_time)
    ORDER BY (movement_date_key, location_key, product_key, document_id)""",
]

DIMENSIONS = ["dim_date", "dim_product", "dim_location", "dim_movement_type", "dim_supplier"]

DDL += [f"CREATE TABLE IF NOT EXISTS {dim}_stage AS {dim}" for dim in DIMENSIONS]

S3_STRUCTURE = ", ".join(f"{name} {kind}" for name, kind in STG_COLUMNS)

LOAD_STG = f"""
INSERT INTO stg_movement
SELECT {", ".join(name for name, _ in STG_COLUMNS)}, _file
FROM s3(
    {S3_COLLECTION},
    filename = '{S3_BUCKET}/{S3_PREFIX}/*.jsonl',
    format = 'JSONEachRow',
    structure = '{S3_STRUCTURE}'
)
SETTINGS input_format_skip_unknown_fields = 1
"""

SOURCE = "(SELECT * FROM stg_movement ORDER BY event_id, source_file LIMIT 1 BY event_id)"


def location_key(column):
    number = f"toUInt32OrZero(extract({column}, '\\\\d+'))"
    return (
        f"toUInt32(multiIf({column} = 'EXTERNAL', 0, "
        f"match({column}, '^WH-?\\\\d+$'), 1000000 + {number}, "
        f"match({column}, '^ST-?\\\\d+$'), {number}, "
        f"match({column}, '^(DC|RC)-?\\\\d+$'), 2000000 + {number}, "
        f"3000000000 + cityHash64({column}) % 1000000000))"
    )


def location_type(column):
    return (
        f"multiIf(startsWith({column}, 'WH'), 'warehouse', startsWith({column}, 'ST'), 'store', "
        f"match({column}, '^(DC|RC)'), 'dc', 'unknown')"
    )


PRODUCT_KEY = (
    "toUInt32(if(match(product_id, '\\\\d'), toUInt32OrZero(extract(product_id, '\\\\d+')), "
    "cityHash64(product_id) % 4000000000))"
)

MOVEMENT_GROUP = """multiIf(
    positionCaseInsensitiveUTF8(movement_type, 'перемещ') > 0, 'TRANSFER',
    positionCaseInsensitiveUTF8(movement_type, 'возврат') > 0, 'RETURN',
    positionCaseInsensitiveUTF8(movement_type, 'поступ') > 0, 'RECEIPT',
    positionCaseInsensitiveUTF8(movement_type, 'спис') > 0, 'WRITE_OFF',
    positionCaseInsensitiveUTF8(movement_type, 'инвент') > 0, 'ADJUSTMENT',
    'OTHER')"""

DIM_BUILD = {
    "dim_date": f"""
    INSERT INTO dim_date_stage
    SELECT
        toUInt32(formatDateTime(d, '%Y%m%d')),
        d,
        toDayOfMonth(d),
        toISOWeek(d),
        toMonth(d),
        toQuarter(d),
        toYear(d)
    FROM (SELECT DISTINCT toDate(parseDateTime64BestEffort(event_time, 6, 'UTC')) AS d FROM {SOURCE})
    """,
    "dim_product": f"""
    INSERT INTO dim_product_stage
    SELECT
        {PRODUCT_KEY},
        product_id,
        argMax(product_name, event_time),
        argMax(product_brand, event_time),
        argMax(product_category, event_time)
    FROM {SOURCE}
    GROUP BY product_id
    """,
    "dim_location": f"""
    INSERT INTO dim_location_stage
    SELECT 0, 'EXTERNAL', 'external', 'Внешняя локация (поставщик / покупатель)', ''
    UNION ALL
    SELECT
        {location_key("location_id")},
        location_id,
        {location_type("location_id")},
        argMax(name, event_time),
        argMax(city, event_time)
    FROM (
        SELECT assumeNotNull(from_warehouse_id) AS location_id,
               ifNull(from_warehouse_name, '') AS name,
               ifNull(from_warehouse_region, '') AS city,
               event_time
        FROM {SOURCE}
        WHERE from_warehouse_id IS NOT NULL
        UNION ALL
        SELECT assumeNotNull(to_warehouse_id),
               ifNull(to_warehouse_name, ''),
               ifNull(to_warehouse_region, ''),
               event_time
        FROM {SOURCE}
        WHERE to_warehouse_id IS NOT NULL
    )
    GROUP BY location_id
    """,
    "dim_movement_type": "INSERT INTO dim_movement_type_stage VALUES "
    + ", ".join(f"({k}, '{code}', '{group}', {sign}, {internal})" for k, code, group, sign, internal in MOVEMENT_TYPES),
    "dim_supplier": "INSERT INTO dim_supplier_stage VALUES (0, 'N/A', 'Не применимо', '', '')",
}

FACT_BUILD = f"""
    INSERT INTO fact_inventory_movement
    SELECT
        f.movement_date_key,
        f.product_key,
        {location_key("f.location_id")},
        {location_key("f.counter_location_id")},
        mt.movement_type_key,
        0,
        f.movement_id,
        1,
        f.event_id,
        f.event_ts,
        toDecimal64(greatest(f.net, 0), 3),
        toDecimal64(greatest(-f.net, 0), 3),
        toDecimal64(f.net, 3),
        toDecimal64(f.unit_cost, 2),
        toDecimal64(f.net * f.unit_cost, 2)
    FROM (
        SELECT
            event_id,
            movement_id,
            event_ts,
            toUInt32(formatDateTime(toDate(event_ts), '%Y%m%d')) AS movement_date_key,
            {PRODUCT_KEY} AS product_key,
            unit_cost,
            tupleElement(side, 1) AS location_id,
            tupleElement(side, 2) AS counter_location_id,
            tupleElement(side, 3) AS net,
            if(grp IN ('TRANSFER', 'RETURN', 'ADJUSTMENT', 'OTHER'),
               concat(grp, if(net > 0, '_IN', '_OUT')),
               grp) AS type_code
        FROM (
            SELECT
                *,
                parseDateTime64BestEffort(event_time, 6, 'UTC') AS event_ts,
                toFloat64(abs(quantity)) AS q,
                abs(cost_amount / quantity) AS unit_cost,
                {MOVEMENT_GROUP} AS grp
            FROM {SOURCE}
            WHERE quantity != 0
              AND (from_warehouse_id IS NOT NULL OR to_warehouse_id IS NOT NULL)
              AND NOT ifNull(from_warehouse_id = to_warehouse_id, 0)
        )
        ARRAY JOIN multiIf(
            from_warehouse_id IS NOT NULL AND to_warehouse_id IS NOT NULL,
                [(assumeNotNull(from_warehouse_id), assumeNotNull(to_warehouse_id), -q),
                 (assumeNotNull(to_warehouse_id), assumeNotNull(from_warehouse_id), q)],
            to_warehouse_id IS NOT NULL,
                [(assumeNotNull(to_warehouse_id), 'EXTERNAL', q)],
                [(assumeNotNull(from_warehouse_id), 'EXTERNAL', -q)]
        ) AS side
    ) AS f
    INNER JOIN dim_movement_type AS mt ON mt.type_code = f.type_code
    """

CHECKS = {
    "строк в stg / уникальных событий": "SELECT count(), uniqExact(event_id) FROM stg_movement",
    "строк в факте": "SELECT count() FROM fact_inventory_movement",
    "товаров / локаций / дат": (
        "SELECT (SELECT count() FROM dim_product), (SELECT count() FROM dim_location), "
        "(SELECT count() FROM dim_date)"
    ),
    "внутренние движения по сети (должно быть 0)": (
        "SELECT sum(f.net_qty), sum(f.net_cost_amount) FROM fact_inventory_movement AS f "
        "INNER JOIN dim_movement_type AS mt ON mt.movement_type_key = f.movement_type_key "
        "WHERE mt.is_internal = 1"
    ),
    "строк по типам движения": (
        "SELECT mt.type_code, count() FROM fact_inventory_movement AS f "
        "INNER JOIN dim_movement_type AS mt ON mt.movement_type_key = f.movement_type_key "
        "GROUP BY mt.type_code ORDER BY mt.type_code"
    ),
}


def rabbitmq_to_s3(**context):
    import boto3
    import pika
    import urllib3
    from airflow.models import Variable

    credentials = pika.PlainCredentials(
        Variable.get("rabbit_mq_svc_login"),
        Variable.get("rabbit_mq_svc_password"),
    )
    parameters = pika.ConnectionParameters(
        host=Variable.get("rabbit_mq_host"),
        port=5672,
        virtual_host="/",
        credentials=credentials,
        heartbeat=600,
        blocked_connection_timeout=300,
    )
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()
    channel.basic_qos(prefetch_count=MAX_MESSAGES)

    messages, skipped, last_tag = [], 0, None
    try:
        for method, _properties, body in channel.consume(QUEUE, auto_ack=False, inactivity_timeout=5):
            if method is None:
                break
            last_tag = method.delivery_tag
            try:
                messages.append(json.loads(body.decode("utf-8")))
            except (UnicodeDecodeError, json.JSONDecodeError):
                skipped += 1
            if len(messages) + skipped >= MAX_MESSAGES:
                break
    finally:
        if channel.is_open:
            if last_tag is not None:
                channel.basic_nack(delivery_tag=last_tag, multiple=True, requeue=True)
            channel.cancel()
        connection.close()

    print(f"RabbitMQ: прочитано {len(messages)} сообщений из {QUEUE}, не JSON: {skipped}; "
          f"все сообщения возвращены в очередь")
    if not messages:
        print("Очередь пуста, файл в S3 не пишем")
        return None

    messages.sort(key=lambda m: str(m.get("event_id")))
    payload = "\n".join(json.dumps(m, ensure_ascii=False) for m in messages).encode("utf-8")
    key = f"{S3_PREFIX}/movement_{hashlib.md5(payload).hexdigest()[:16]}.jsonl"

    urllib3.disable_warnings()
    s3 = boto3.session.Session().client(
        service_name="s3",
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id=Variable.get("s3_dad_ksu_dataplatform_access_key"),
        aws_secret_access_key=Variable.get("s3_dad_ksu_dataplatform_secret_key"),
        verify=False,
    )
    s3.put_object(Bucket=S3_BUCKET, Key=key, Body=payload, ContentType="application/x-ndjson")
    print(f"S3: записано {len(messages)} строк в s3://{S3_BUCKET}/{key}")
    return key


def s3_to_clickhouse(**context):
    from airflow.models import Variable
    from clickhouse_driver.client import Client

    with Client(
        host=Variable.get("clickhouse_host"),
        user=Variable.get("clickhouse_tech_user_account"),
        password=Variable.get("clickhouse_tech_user_password"),
        database=CH_DATABASE,
        port=9000,
    ) as client:
        for statement in DDL:
            client.execute(statement)

        client.execute("TRUNCATE TABLE stg_movement")
        client.execute(LOAD_STG)
        staged = client.execute("SELECT count() FROM stg_movement")[0][0]
        print(f"ClickHouse: в stg_movement загружено {staged} строк из s3://{S3_BUCKET}/{S3_PREFIX}/")
        if staged == 0:
            print("Файлов в S3 нет, справочники и факт не трогаем")
            return

        for dim in DIMENSIONS:
            stage = f"{dim}_stage"
            client.execute(f"TRUNCATE TABLE {stage}")
            client.execute(DIM_BUILD[dim])
            client.execute(f"EXCHANGE TABLES {dim} AND {stage}")
            client.execute(f"TRUNCATE TABLE {stage}")
            rows = client.execute(f"SELECT count() FROM {dim}")[0][0]
            print(f"{dim}: {rows} строк, подменён через EXCHANGE TABLES с {stage}")

        client.execute("TRUNCATE TABLE fact_inventory_movement")
        client.execute(FACT_BUILD)

        for title, query in CHECKS.items():
            print(f"{title}: {client.execute(query)}")
