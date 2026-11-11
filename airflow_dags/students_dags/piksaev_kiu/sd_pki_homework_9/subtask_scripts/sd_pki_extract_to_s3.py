import hashlib
import json

RABBITMQ_HOST = "rabbitmq.student.magnit.ru"
RABBITMQ_PORT = 5672
RABBITMQ_VHOST = "/"
RABBITMQ_QUEUE = "sales.queue"

S3_ENDPOINT = "https://storage.yandexcloud.net"
S3_BUCKET = "dad-ksu-datalab"
S3_PREFIX = "DATA/piksaev_kiu"

MAX_MESSAGES = 10000


def _s3_client():
    import boto3
    import urllib3
    from airflow.models import Variable

    urllib3.disable_warnings()
    return boto3.session.Session().client(
        service_name="s3",
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id=Variable.get("s3_dad_ksu_dataplatform_access_key"),
        aws_secret_access_key=Variable.get("s3_dad_ksu_dataplatform_secret_key"),
        verify=False,
    )


def _rabbit_connection():
    import pika
    from airflow.models import Variable

    credentials = pika.PlainCredentials(
        Variable.get("rabbit_mq_svc_login"),
        Variable.get("rabbit_mq_svc_password"),
    )
    return pika.BlockingConnection(
        pika.ConnectionParameters(
            host=Variable.get("rabbit_mq_host", default_var=RABBITMQ_HOST),
            port=RABBITMQ_PORT,
            virtual_host=RABBITMQ_VHOST,
            credentials=credentials,
            heartbeat=600,
        )
    )


def extract_rabbitmq_to_s3(**context):
    connection = _rabbit_connection()
    channel = connection.channel()

    lines = []
    delivery_tags = []

    try:
        channel.queue_declare(queue=RABBITMQ_QUEUE, passive=True)

        while len(lines) < MAX_MESSAGES:
            method, properties, body = channel.basic_get(
                queue=RABBITMQ_QUEUE, auto_ack=False
            )
            if method is None:
                break

            delivery_tags.append(method.delivery_tag)

            try:
                message = json.loads(body.decode("utf-8"))
            except ValueError:
                print(f"RabbitMQ: не-JSON сообщение, пропущено: {method.delivery_tag}")
                continue

            lines.append(json.dumps(message, ensure_ascii=False))

        print(f"RabbitMQ: прочитано сообщений: {len(lines)}")

        if not lines:
            print("RabbitMQ: очередь пуста, файл в S3 не создаём")
            return

        payload = "\n".join(lines).encode("utf-8")
        digest = hashlib.md5(payload).hexdigest()[:16]
        key = f"{S3_PREFIX}/events_{digest}.jsonl"

        _s3_client().put_object(
            Bucket=S3_BUCKET,
            Key=key,
            Body=payload,
            ContentType="application/x-ndjson",
        )
        print(f"S3: загружено {len(lines)} строк в s3://{S3_BUCKET}/{key}")

    finally:
        requeued = 0
        for tag in delivery_tags:
            try:
                channel.basic_nack(delivery_tag=tag, requeue=True)
                requeued += 1
            except Exception as error:
                print(f"RabbitMQ: не удалось вернуть сообщение {tag}: {error}")
        if requeued:
            print(f"RabbitMQ: возвращено в очередь: {requeued}")

        try:
            connection.close()
        except Exception:
            pass
