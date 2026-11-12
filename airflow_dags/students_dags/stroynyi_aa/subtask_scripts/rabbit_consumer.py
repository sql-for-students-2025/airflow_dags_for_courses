import json
import os
import tempfile
import pika
from airflow.models import Variable

def consume_rabbitmq_to_file(**context) -> None:
    login = Variable.get("rabbit_mq_svc_login")
    password = Variable.get("rabbit_mq_svc_password")
    host = Variable.get("rabbit_mq_host")

    queue = "sales.queue"
    max_messages = 100

    credentials = pika.PlainCredentials(login, password)
    params = pika.ConnectionParameters(host=host, port=5672, virtual_host="/", credentials=credentials)

    collected = []
    connection = pika.BlockingConnection(params)
    channel = connection.channel()
    channel.basic_qos(prefetch_count=1)

    def callback(ch, method, properties, body):
        try:
            msg = json.loads(body.decode("utf-8"))
            collected.append(msg)
            ch.basic_ack(delivery_tag=method.delivery_tag)
        except json.JSONDecodeError:
            ch.basic_ack(delivery_tag=method.delivery_tag)

        if len(collected) >= max_messages:
            ch.stop_consuming()

    channel.basic_consume(queue=queue, on_message_callback=callback, auto_ack=False)
    try:
        channel.start_consuming()
    finally:
        connection.close()

    # Формируем имя файла на основе run_id (без XCom)
    run_id = context["run_id"].replace(":", "_").replace("+", "_")
    out_dir = tempfile.mkdtemp(prefix="rabbit_")
    # Имя файла всегда одинаковое для текущего запуска
    out_path = os.path.join(out_dir, f"{run_id}.json")
    
    with open(out_path, "w", encoding="utf-8") as f:
        for row in collected:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")