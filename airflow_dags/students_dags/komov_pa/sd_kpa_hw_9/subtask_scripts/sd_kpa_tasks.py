def extract_rabbitmq_to_s3(**kwargs):
    import json
    import uuid
    import pika
    from airflow.models import Variable
    from airflow.providers.amazon.aws.hooks.s3 import S3Hook

    rmq_user = Variable.get('rabbit_mq_svc_login')
    rmq_pass = Variable.get('rabbit_mq_svc_password')
    
    rmq_host = Variable.get('komov_pa_rabbitmq_host')
    rmq_port = int(Variable.get('komov_pa_rabbitmq_port', default_var='5672'))
    rmq_vhost = Variable.get('komov_pa_rabbitmq_vhost', default_var='/')
    s3_bucket = Variable.get('komov_pa_s3_bucket')
    
    queue_name = 'returns.queue'
    
    credentials = pika.PlainCredentials(rmq_user, rmq_pass)
    parameters = pika.ConnectionParameters(
        host=rmq_host,
        port=rmq_port,
        virtual_host=rmq_vhost,
        credentials=credentials
    )
    
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()
    
    messages = []
    for _ in range(100):
        method_frame, header_frame, body = channel.basic_get(queue=queue_name, auto_ack=False)
        if method_frame:
            messages.append(json.loads(body.decode('utf-8')))
            channel.basic_ack(delivery_tag=method_frame.delivery_tag)
        else:
            break
    connection.close()

    if not messages:
        return None

    file_name = f"returns_{uuid.uuid4().hex}.json"
    s3_key = f"DATA/komov_pa/{file_name}"
    local_path = f"/tmp/{file_name}"

    with open(local_path, 'w', encoding='utf-8') as f:
        for msg in messages:
            f.write(json.dumps(msg, ensure_ascii=False) + '\n')

    hook = S3Hook(aws_conn_id='s3_dad_ksu_dataplatform') 
    hook.load_file(
        filename=local_path,
        key=s3_key,
        bucket_name=s3_bucket,
        replace=True
    )
    print(f"Загружено {len(messages)} сообщений в S3: {s3_key}")
    return s3_key


def load_s3_to_clickhouse(**kwargs):
  
    import json
    import pandas as pd
    import clickhouse_connect
    from airflow.models import Variable
    from airflow.providers.amazon.aws.hooks.s3 import S3Hook

    ti = kwargs['ti']
    s3_key = ti.xcom_pull(task_ids='extract_to_s3')
    if not s3_key: return

    s3_bucket = Variable.get('komov_pa_s3_bucket')
    ch_host = Variable.get('komov_pa_clickhouse_host')
    ch_user = Variable.get('komov_pa_clickhouse_user')
    ch_pass = Variable.get('komov_pa_clickhouse_password')
    ch_port = int(Variable.get('komov_pa_clickhouse_port', default_var='8123'))
    
    hook = S3Hook(aws_conn_id='s3_dad_ksu_dataplatform')
    local_file = hook.download_file(key=s3_key, bucket_name=s3_bucket)

    data = []
    with open(local_file, 'r', encoding='utf-8') as f:
        for line in f:
            data.append(json.loads(line))

    if not data: return
    df = pd.DataFrame(data)

    client = clickhouse_connect.get_client(
        host=ch_host,
        port=ch_port,
        username=ch_user,
        password=ch_pass
    )

    df_product = df[['product_id', 'product_name', 'product_brand', 'product_category']].copy()
    df_product.columns = ['sku', 'product_name', 'brand', 'category']
    df_product.drop_duplicates(subset=['sku'], inplace=True)
    client.insert_df('komov_pa_sch_db.dim_product', df_product)

    df_store = df[['store_id', 'store_name', 'store_city', 'store_region']].copy()
    df_store.columns = ['store_code', 'store_name', 'city', 'region']
    df_store.drop_duplicates(subset=['store_code'], inplace=True)
    client.insert_df('komov_pa_sch_db.dim_store', df_store)

    df_reason = df[['reason_id', 'reason_name']].copy()
    df_reason.columns = ['reason_code', 'reason_name']
    df_reason['reason_group'] = df_reason['reason_name'].apply(lambda x: 'логистика' if 'тот' in x.lower() else 'клиентская')
    df_reason['is_company_fault'] = df_reason['reason_group'].apply(lambda x: 1 if x in ['логистика', 'брак'] else 0)
    df_reason = df_reason[['reason_code', 'reason_group', 'is_company_fault']]
    df_reason.drop_duplicates(subset=['reason_code'], inplace=True)
    client.insert_df('komov_pa_sch_db.dim_return_reason', df_reason)

    df_fact = pd.DataFrame({
        'return_line_id': df.apply(lambda x: f"{x['return_id']}:{x['product_id']}", axis=1),
        'return_doc_no': df['return_id'],
        'event_date': pd.to_datetime(df['event_time']).dt.date,
        'product_sku': df['product_id'],
        'store_code': df['store_id'],
        'customer_id': df['customer_id'],
        'customer_segment': df['customer_segment'],
        'reason_code': df['reason_id'],
        'channel_name': df['channel_name'],
        'returned_qty': df['returned_qty'].astype('uint32'),
        'refund_amount': df['refund_amount'].astype(float),
        'returned_cost': df['returned_cost'].astype(float),
        'writeoff_qty': df['writeoff_qty'].astype('uint32'),
        'days_to_return': df['days_to_return'].astype('uint32')
    })
    client.insert_df('komov_pa_sch_db.fact_return_line', df_fact)
    print(f"Успешно загружено {len(df)} строк в ClickHouse.")