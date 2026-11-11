import json
import uuid
import pika
import pandas as pd
import boto3
import urllib3
from airflow.models import Variable
import clickhouse_connect

urllib3.disable_warnings()

def extract_rabbitmq_to_s3(**kwargs):
    
    rmq_host = Variable.get('rabbit_mq_host')
    rmq_user = Variable.get('rabbit_mq_svc_login')
    rmq_pass = Variable.get('rabbit_mq_svc_password')
    
    access_key = Variable.get('s3_dad_ksu_dataplatform_access_key')
    secret_key = Variable.get('s3_dad_ksu_dataplatform_secret_key')

    queue_name = "returns.queue"
    
   
    credentials = pika.PlainCredentials(rmq_user, rmq_pass)
    parameters = pika.ConnectionParameters(
        host=rmq_host, port=5672, virtual_host="/", credentials=credentials
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
        print("Очередь пуста")
        return None

   
    file_name = f"returns_{uuid.uuid4().hex}.json"
    s3_key = f"DATA/komov_pa/{file_name}"
    local_path = f"/tmp/{file_name}"

    with open(local_path, 'w', encoding='utf-8') as f:
        for msg in messages:
            f.write(json.dumps(msg, ensure_ascii=False) + '\n')

    s3_client = boto3.client(
        service_name='s3',
        endpoint_url='https://storage.yandexcloud.net',
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        verify=False
    )
    
  
    s3_client.upload_file(local_path, 'dad-ksu-datalab', s3_key)
    print(f"Загружено {len(messages)} сообщений в S3: {s3_key}")
    
    return s3_key


def load_s3_to_clickhouse(**kwargs):
   
    access_key = Variable.get('s3_dad_ksu_dataplatform_access_key')
    secret_key = Variable.get('s3_dad_ksu_dataplatform_secret_key')
    
    ch_host = Variable.get('clickhouse_host')
    ch_user = Variable.get('clickhouse_tech_user_account')
    ch_pass = Variable.get('clickhouse_tech_user_password')

    ti = kwargs['ti']
    s3_key = ti.xcom_pull(task_ids='extract_to_s3')
    if not s3_key: return

    s3_client = boto3.client(
        service_name='s3',
        endpoint_url='https://storage.yandexcloud.net',
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        verify=False
    )
    local_file = f"/tmp/{uuid.uuid4().hex}.json"
    s3_client.download_file('dad-ksu-datalab', s3_key, local_file)

    data = []
    with open(local_file, 'r', encoding='utf-8') as f:
        for line in f:
            data.append(json.loads(line))

    df = pd.DataFrame(data)

    client = clickhouse_connect.get_client(
        host=ch_host,
        port=8123,
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