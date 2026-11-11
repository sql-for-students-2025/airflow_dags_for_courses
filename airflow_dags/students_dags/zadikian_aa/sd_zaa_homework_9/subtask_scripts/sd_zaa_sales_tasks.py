import json


def rabbitmq_to_s3(run_id):
    import boto3
    import pika
    from airflow.models import Variable

    connection = pika.BlockingConnection(pika.ConnectionParameters(
        host=Variable.get('rabbit_mq_host'),
        port=5672,
        virtual_host='/',
        credentials=pika.PlainCredentials(
            Variable.get('rabbit_mq_svc_login'), Variable.get('rabbit_mq_svc_password'),
        ),
    ))
    try:
        channel = connection.channel()
        channel.basic_qos(prefetch_count=200)
        messages = {}
        last_tag = None
        for count, (method, properties, body) in enumerate(
            channel.consume('sales.queue', inactivity_timeout=10), 1
        ):
            if method is None:
                break
            last_tag = method.delivery_tag
            try:
                message = json.loads(body)
                if not isinstance(message, dict):
                    raise ValueError('Ожидался JSON-объект')
                messages.setdefault(message.get('event_id') or last_tag, message)
            except (ValueError, UnicodeDecodeError) as error:
                print('Ошибка сообщения:', error)
            if count == 200:
                break

        key = None
        if messages:
            s3 = boto3.client(
                's3', endpoint_url='https://storage.yandexcloud.net',
                aws_access_key_id=Variable.get('s3_dad_ksu_dataplatform_access_key'),
                aws_secret_access_key=Variable.get('s3_dad_ksu_dataplatform_secret_key'),
            )
            filename = run_id.replace(':', '_').replace('/', '_')
            key = f'DATA/zadikian_aa/{filename}.jsonl'
            data = '\n'.join(json.dumps({'message': json.dumps(m)}) for m in messages.values())
            s3.put_object(Bucket='dad-ksu-datalab', Key=key, Body=data.encode())
        if last_tag is not None:
            channel.basic_nack(delivery_tag=last_tag, multiple=True, requeue=True)
        print('Сохранено сообщений:', len(messages))
        return key
    finally:
        if connection.is_open:
            connection.close()


def s3_to_clickhouse(ti):
    from clickhouse_driver import Client
    from airflow.models import Variable

    key = ti.xcom_pull(task_ids='rabbitmq_to_s3')
    if not key:
        return
    client = Client(
        host='111.88.243.32',
        port=9000,
        user=Variable.get('clickhouse_tech_user_account'),
        password=Variable.get('clickhouse_tech_user_password'),
        database='zadikian_aa_db',
    )
    try:
        parameters = {'filename': 'dad-ksu-datalab/' + key}
        statements = (
            """
            INSERT INTO stg_sales (source_file, message)
            SELECT DISTINCT %(filename)s, message
            FROM s3(
                s3_dad_ksu_datalab,
                filename=%(filename)s,
                format='JSONEachRow',
                structure='message String'
            )
            WHERE message NOT IN
            (
                SELECT message FROM stg_sales WHERE source_file = %(filename)s
            )
            """,
            """
            CREATE TEMPORARY TABLE sales_batch ENGINE = Memory AS
            SELECT
                *,
                toUInt32(toYYYYMMDD(event_time)) AS date_id,
                ifNull(
                    CAST(round(toDecimal128(gross_amount, 8) / nullIf(quantity, 0), 2) AS Nullable(Decimal(18, 2))),
                    toDecimal64(0, 2)
                ) AS price,
                net_amount - cost_amount AS margin_amount
            FROM
            (
                SELECT
                    message,
                    parseDateTime64BestEffortOrNull(
                        JSONExtractString(message, 'event_time'), 6, 'UTC'
                    ) AS event_time,
                    JSONExtractString(message, 'receipt_status') AS receipt_status,
                    toInt64OrNull(coalesce(
                        nullIf(JSONExtractString(message, 'quantity'), ''),
                        JSONExtractRaw(message, 'quantity')
                    )) AS quantity,
                    toUInt64OrNull(replaceRegexpAll(
                        coalesce(nullIf(JSONExtractString(message, 'store_id'), ''),
                                 JSONExtractRaw(message, 'store_id')), '[^0-9]', ''
                    )) AS store_id,
                    toUInt64OrNull(replaceRegexpAll(
                        coalesce(nullIf(JSONExtractString(message, 'product_id'), ''),
                                 JSONExtractRaw(message, 'product_id')), '[^0-9]', ''
                    )) AS product_id,
                    ifNull(toUInt64OrNull(replaceRegexpAll(
                        coalesce(nullIf(JSONExtractString(message, 'customer_id'), ''),
                                 JSONExtractRaw(message, 'customer_id')), '[^0-9]', ''
                    )), toUInt64(0)) AS customer_id,
                    JSONExtractString(message, 'store_name') AS store_name,
                    JSONExtractString(message, 'store_city') AS city,
                    JSONExtractString(message, 'store_region') AS region,
                    JSONExtractString(message, 'product_name') AS product_name,
                    JSONExtractString(message, 'product_category') AS category,
                    CAST(round(toDecimal128OrNull(coalesce(
                        nullIf(JSONExtractString(message, 'gross_amount'), ''),
                        JSONExtractRaw(message, 'gross_amount')
                    ), 8), 2) AS Nullable(Decimal(18, 2))) AS gross_amount,
                    CAST(round(toDecimal128OrNull(coalesce(
                        nullIf(JSONExtractString(message, 'discount_amount'), ''),
                        JSONExtractRaw(message, 'discount_amount')
                    ), 8), 2) AS Nullable(Decimal(18, 2))) AS discount_amount,
                    CAST(round(toDecimal128OrNull(coalesce(
                        nullIf(JSONExtractString(message, 'net_amount'), ''),
                        JSONExtractRaw(message, 'net_amount')
                    ), 8), 2) AS Nullable(Decimal(18, 2))) AS net_amount,
                    CAST(round(toDecimal128OrNull(coalesce(
                        nullIf(JSONExtractString(message, 'cost_amount'), ''),
                        JSONExtractRaw(message, 'cost_amount')
                    ), 8), 2) AS Nullable(Decimal(18, 2))) AS cost_amount
                FROM stg_sales
                WHERE source_file = %(filename)s
                  AND isValidJSON(message)
                  AND JSONExtractString(message, 'event_type') = 'receipt_line'
            )
            WHERE event_time IS NOT NULL
              AND quantity IS NOT NULL AND quantity >= 0
              AND store_id IS NOT NULL AND product_id IS NOT NULL
              AND gross_amount IS NOT NULL AND discount_amount IS NOT NULL
              AND net_amount IS NOT NULL AND cost_amount IS NOT NULL
            """,
            """
            INSERT INTO dim_date (date_id, full_date, year, month, day, day_of_week)
            SELECT DISTINCT
                date_id, toDate(event_time), toYear(event_time), toMonth(event_time),
                toDayOfMonth(event_time),
                ['понедельник', 'вторник', 'среда', 'четверг', 'пятница', 'суббота', 'воскресенье'][toDayOfWeek(event_time)]
            FROM sales_batch
            WHERE date_id NOT IN (SELECT date_id FROM dim_date)
            """,
            """
            INSERT INTO dim_store (store_id, store_name, address, city, region)
            SELECT store_id, attributes.1, attributes.2, attributes.3, attributes.4
            FROM
            (
                SELECT store_id, argMax(
                    tuple(store_name, arrayStringConcat(arrayFilter(x -> x != '', [city, region]), ', '), city, region),
                    tuple(event_time, message)
                ) AS attributes
                FROM sales_batch
                WHERE store_id NOT IN (SELECT store_id FROM dim_store)
                GROUP BY store_id
            )
            """,
            """
            INSERT INTO dim_product (product_id, name, category, price)
            SELECT product_id, attributes.1, attributes.2, attributes.3
            FROM
            (
                SELECT product_id, argMax(tuple(product_name, category, price), tuple(event_time, message)) AS attributes
                FROM sales_batch
                WHERE product_id NOT IN (SELECT product_id FROM dim_product)
                GROUP BY product_id
            )
            """,
            """
            INSERT INTO dim_customer (customer_id, name, email, phone)
            SELECT DISTINCT customer_id, '', '', ''
            FROM sales_batch
            WHERE customer_id NOT IN (SELECT customer_id FROM dim_customer)
            """,
            """
            CREATE TEMPORARY TABLE new_sales ENGINE = Memory AS
            SELECT
                date_id, store_id, product_id, customer_id, quantity,
                gross_amount, discount_amount, net_amount, margin_amount,
                min(event_time) AS event_time
            FROM sales_batch
            WHERE receipt_status != 'CANCELLED'
              AND (date_id, store_id, product_id, customer_id, quantity,
                   gross_amount, discount_amount, net_amount, margin_amount) NOT IN
              (
                  SELECT date_id, store_id, product_id, customer_id, quantity,
                         gross_amount, discount_amount, net_amount, margin_amount
                  FROM fact_sales
              )
            GROUP BY date_id, store_id, product_id, customer_id, quantity,
                     gross_amount, discount_amount, net_amount, margin_amount
            """,
            """
            INSERT INTO fact_sales
                (sale_id, date_id, store_id, product_id, customer_id, quantity,
                 gross_amount, discount_amount, net_amount, margin_amount)
            SELECT
                (SELECT coalesce(max(sale_id), 0) FROM fact_sales)
                    + row_number() OVER (ORDER BY event_time, date_id, store_id, product_id,
                                        customer_id, quantity, gross_amount, discount_amount,
                                        net_amount, margin_amount),
                date_id, store_id, product_id, customer_id, quantity,
                gross_amount, discount_amount, net_amount, margin_amount
            FROM new_sales
            """,
        )
        for statement in statements:
            client.execute(statement, params=parameters)
        print('Загрузка из S3 в в стейдж завершена, ключ :', key)
    finally:
        client.disconnect()
