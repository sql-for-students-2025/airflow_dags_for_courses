import json
import os
import pika
import boto3
import urllib3
from datetime import datetime
from airflow.models import Variable

urllib3.disable_warnings()

def fetch_rabbit_to_s3(**kwargs):
    # Настройки RabbitMQ
    rabbit_host = Variable.get("rabbit_mq_host")
    rabbit_user = Variable.get("rabbit_mq_svc_login")
    rabbit_pass = Variable.get("rabbit_mq_svc_password")
    queue_name = "sales.queue"
    
    # Настройки S3
    endpoint_url = 'https://storage.yandexcloud.net'
    access = Variable.get("s3_dad_ksu_dataplatform_access_key")
    secret = Variable.get("s3_dad_ksu_dataplatform_secret_key")
    bucket = 'dad-ksu-datalab'
    
    # Путь в S3: DATA/ivanenko_ad/...
    login = kwargs.get('login', 'ivanenko_ad')
    s3_path = f"DATA/{login}/sales_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    
    # 1. Подключение к RabbitMQ
    credentials = pika.PlainCredentials(rabbit_user, rabbit_pass)
    parameters = pika.ConnectionParameters(host=rabbit_host, credentials=credentials)
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()
    
    collected = []
    MAX_MESSAGES = 999
    
    def callback(ch, method, properties, body):
        try:
            msg = json.loads(body.decode("utf-8"))
            collected.append(msg)
        except json.JSONDecodeError:
            pass
        # Возвращаем в очередь
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
        if len(collected) >= MAX_MESSAGES:
            ch.stop_consuming()
            
    channel.basic_consume(queue=queue_name, on_message_callback=callback, auto_ack=False)
    channel.start_consuming()
    connection.close()
    
    # 2. Запись в S3
    session = boto3.session.Session()
    s3 = session.client(
        service_name='s3', endpoint_url=endpoint_url,
        aws_access_key_id=access, aws_secret_access_key=secret, verify=False
    )
    
    json_data = json.dumps(collected, ensure_ascii=False, indent=2)
    s3.put_object(Bucket=bucket, Key=s3_path, Body=json_data)
    
    print(f"✅ Загружено {len(collected)} записей в S3: {s3_path}")
    return s3_path