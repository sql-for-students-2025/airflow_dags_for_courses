def extract_rabbitmq_to_s3(**kwargs):
    import json
    import uuid
    import pika
    import boto3
    from airflow.models import Variable
    from botocore.client import Config

    rmq_user = Variable.get('rabbit_mq_svc_login')
    rmq_pass = Variable.get('rabbit_mq_svc_password')
    rmq_host = Variable.get('komov_pa_rabbitmq_host')
    s3_bucket = Variable.get('komov_pa_s3_bucket')
    
    aws_access = Variable.get('s3_dad_ksu_dataplatform_access_key')
    aws_secret = Variable.get('s3_dad_ksu_dataplatform_secret_key')

    credentials = pika.PlainCredentials(rmq_user, rmq_pass)
    parameters = pika.ConnectionParameters(
        host=rmq_host, port=5672, virtual_host='/', credentials=credentials
    )
    
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()
    
    messages = []
    for _ in range(100):
        method_frame, header_frame, body = channel.basic_get(queue='returns.queue', auto_ack=False)
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
        aws_access_key_id=aws_access,
        aws_secret_access_key=aws_secret,
        verify=False,
        config=Config(signature_version='s3v4')
    )
    
    s3_client.upload_file(local_path, s3_bucket, s3_key)
    print(f"Загружено {len(messages)} сообщений в S3: {s3_key}")
    
    return s3_key