import json
import re
from datetime import datetime
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP


def rabbitmq_to_s3(run_id):
    import boto3
    import pika
    from airflow.models import Variable

    connection = pika.BlockingConnection(pika.ConnectionParameters(
        host='rabbitmq.student.magnit.ru',
        port=int(Variable.get('zadikian_aa_rabbitmq_port', default_var='5672')),
        virtual_host=Variable.get('zadikian_aa_rabbitmq_vhost', default_var='/'),
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
    import clickhouse_connect
    from airflow.models import Variable

    key = ti.xcom_pull(task_ids='rabbitmq_to_s3')
    if not key:
        return
    client = clickhouse_connect.get_client(
        host=Variable.get('zadikian_aa_clickhouse_host', default_var='111.88.243.32'),
        port=int(Variable.get('zadikian_aa_clickhouse_port', default_var='8123')),
        username=Variable.get('zadikian_aa_clickhouse_login'),
        password=Variable.get('zadikian_aa_clickhouse_password'),
        database='zadikian_aa_db',
    )
    try:
        messages = client.query(
            "SELECT message FROM s3(s3_dad_ksu_datalab, filename={filename:String}, "
            "format='JSONEachRow', structure='message String')",
            parameters={'filename': 'dad-ksu-datalab/' + key},
        ).result_rows
        tables = {'dim_date': [], 'dim_store': [], 'dim_product': [], 'dim_customer': [], 'fact_sales': []}
        sales = []
        days = ['понедельник', 'вторник', 'среда', 'четверг', 'пятница', 'суббота', 'воскресенье']

        for (body,) in messages:
            m = json.loads(body)
            if m.get('event_type') != 'receipt_line':
                continue
            try:
                date = datetime.fromisoformat(m['event_time'].replace('Z', '+00:00'))
                quantity = int(m['quantity'])
                if quantity < 0:
                    raise ValueError('Отрицательное количество')
                gross, discount, net, cost = [
                    Decimal(str(m[field])).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)
                    for field in ('gross_amount', 'discount_amount', 'net_amount', 'cost_amount')
                ]
                price = (gross / quantity).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP) if quantity else Decimal(0)
                if not all(value.is_finite() for value in (gross, discount, net, cost, price)):
                    raise ValueError('Некорректная сумма')
                store_id = int(re.sub(r'\D', '', str(m['store_id'] or '')))
                product_id = int(re.sub(r'\D', '', str(m['product_id'] or '')))
                customer_id = int(re.sub(r'\D', '', str(m.get('customer_id') or '')) or '0')
            except (KeyError, ValueError, TypeError, InvalidOperation) as error:
                print('Ошибка:', m.get('event_id'), error)
                continue

            date_id = int(date.strftime('%Y%m%d'))
            city, region = m.get('store_city') or '', m.get('store_region') or ''
            tables['dim_date'].append(dict(date_id=date_id, full_date=date.date(), year=date.year,
                                          month=date.month, day=date.day, day_of_week=days[date.weekday()]))
            tables['dim_store'].append(dict(store_id=store_id, store_name=m.get('store_name') or '',
                                           address=', '.join(x for x in (city, region) if x), city=city, region=region))
            tables['dim_product'].append(dict(product_id=product_id, name=m.get('product_name') or '',
                                             category=m.get('product_category') or '', price=price))
            tables['dim_customer'].append(dict(customer_id=customer_id, name='', email='', phone=''))
            if m.get('receipt_status') != 'CANCELLED':
                sales.append((date, dict(date_id=date_id, store_id=store_id, product_id=product_id,
                                         customer_id=customer_id, quantity=quantity, gross_amount=gross,
                                         discount_amount=discount, net_amount=net, margin_amount=net - cost)))

        tables['fact_sales'] = [row for date, row in sorted(sales, key=lambda sale: sale[0])]
        for table, rows in tables.items():
            if not rows:
                continue
            columns = list(rows[0])
            id_column = columns[0]
            check_columns = columns if table == 'fact_sales' else [id_column]
            existing = client.query(
                f"SELECT {', '.join(check_columns)} FROM {table} WHERE {id_column} IN %(ids)s",
                parameters={'ids': tuple({row[id_column] for row in rows})},
            ).result_rows
            new_rows = {}
            for row in rows:
                row_key = tuple(row[column] for column in check_columns)
                if row_key not in existing:
                    new_rows[row_key] = list(row.values())
            data = list(new_rows.values())
            if table == 'fact_sales':
                sale_id = client.query('SELECT coalesce(max(sale_id), 0) FROM fact_sales').result_rows[0][0]
                data = [[sale_id + i, *row] for i, row in enumerate(data, 1)]
                columns = ['sale_id', *columns]
            if data:
                client.insert(table, data, column_names=columns)
            print(table, 'добавлено:', len(data))
    finally:
        client.close()
