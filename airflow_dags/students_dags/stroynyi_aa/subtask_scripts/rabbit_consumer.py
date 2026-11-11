import json
import os
import tempfile

import pika
from airflow.models import Variable


def consume_rabbitmq_to_file(**context) -> str:
    """
    Читает N сообщений из sales.queue и пишет их в локальный NDJSON-файл.
    Возвращает путь к файлу (через XCom).
    """
    login = Variable.get("rabbit_mq_svc_login")
    password = Variable.get("rabbit_mq_svc_password")
    host = Variable.get("rabbit_mq_host")

    queue = "sales.queue"
    max_messages = 100

    credentials = pika.PlainCredentials(login, password)
    params = pika.ConnectionParameters(
        host=host, port=5672, virtual_host="/", credentials=credentials
    )

    collected = []
    connection = pika.BlockingConnection(params)
    channel = connection.channel()
    channel.basic_qos(prefetch_count=1)

    def callback(ch, method, properties, body):
        try:
            msg = json.loads(body.decode("utf-8"))
        except json.JSONDecodeError:
            # битые сообщения подтверждаем и выкидываем, иначе они зациклятся
            ch.basic_ack(delivery_tag=method.delivery_tag)
            return

        collected.append(msg)
        # ВАЖНО: ack, а не nack с requeue=True — иначе бесконечный цикл,
        # как было в ноутбуке (одно и то же event_id 100 раз).
        ch.basic_ack(delivery_tag=method.delivery_tag)

        if len(collected) >= max_messages:
            ch.stop_consuming()

    channel.basic_consume(
        queue=queue, on_message_callback=callback, auto_ack=False
    )
    try:
        channel.start_consuming()
    finally:
        connection.close()

    # пишем NDJSON (JSONEachRow) — родной формат для ClickHouse
    out_dir = tempfile.mkdtemp(prefix="rabbit_")
    out_path = os.path.join(out_dir, "sales_batch.json")
    with open(out_path, "w", encoding="utf-8") as f:
        for row in collected:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")

    # отдаём путь через XCom следующей задаче
    context["ti"].xcom_push(key="local_file", value=out_path)
    return out_path