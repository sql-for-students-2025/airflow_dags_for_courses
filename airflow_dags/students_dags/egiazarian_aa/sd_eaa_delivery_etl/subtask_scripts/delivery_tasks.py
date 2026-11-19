import json
import re
import zlib
from datetime import datetime
from decimal import Decimal

import boto3
import pika
from airflow.models import Variable
from clickhouse_driver.client import Client

# ---------- Настройки (проверьте по cw.md!) ----------
QUEUE_NAME = "delivery.queue"  # <-- Впишите ваше точное имя очереди RabbitMQ!
S3_BUCKET = "dad-ksu-datalab"
S3_ENDPOINT = Variable.get("s3_endpoint_url", default_var="https://storage.yandexcloud.net")

USER_LOGIN = "egiazarian_aa"
DB_NAME = f"{USER_LOGIN}_db"
MAX_DUPLICATES_IN_A_ROW = 20

MONTH_NAMES = {
    1: "January", 2: "February", 3: "March", 4: "April",
    5: "May", 6: "June", 7: "July", 8: "August",
    9: "September", 10: "October", 11: "November", 12: "December"
}


# ---------- Вспомогательные функции парсинга ----------
def _parse_dt(value: str) -> datetime | None:
    if not value:
        return None
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def _parse_int(value) -> int:
    if value is None:
        return 0
    if isinstance(value, int):
        return value
    digits = re.sub(r"\D", "", str(value))
    return int(digits) if digits else 0


def _parse_decimal(value) -> Decimal | None:
    if value is None:
        return None
    cleaned = re.sub(r"[^\d.,-]", "", str(value)).replace(",", ".")
    try:
        return Decimal(cleaned) if cleaned else None
    except Exception:
        return None


def _make_surrogate_id(key_str: str) -> int:
    """Генерирует стабильный числовой ID из строки для справочников ClickHouse."""
    return zlib.crc32(key_str.encode("utf-8")) & 0xFFFFFFFF


def _get_s3_client():
    access_key = Variable.get("s3_dad_ksu_dataplatform_access_key")
    secret_key = Variable.get("s3_dad_ksu_dataplatform_secret_key")
    return boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
    )


# ---------- ЗАДАЧА 1: Получение данных из RabbitMQ и запись в S3 ----------
def extract_from_rabbit_to_s3(**context) -> str:
    rabbitmq_host = Variable.get("rabbit_mq_host")
    rabbitmq_login = Variable.get("rabbit_mq_svc_login")
    rabbitmq_password = Variable.get("rabbit_mq_svc_password")

    credentials = pika.PlainCredentials(rabbitmq_login, rabbitmq_password)
    parameters = pika.ConnectionParameters(
        host=rabbitmq_host,
        port=5672,
        credentials=credentials,
    )

    collected = []
    seen_ids = set()
    consecutive_duplicates = 0

    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()
    channel.basic_qos(prefetch_count=1)

    def callback(ch, method, properties, body):
        nonlocal consecutive_duplicates
        try:
            message = json.loads(body.decode("utf-8"))
            msg_id = message.get("event_id")

            if msg_id not in seen_ids:
                seen_ids.add(msg_id)
                collected.append(message)
                consecutive_duplicates = 0
            else:
                consecutive_duplicates += 1
        except json.JSONDecodeError:
            pass

        ch.basic_ack(delivery_tag=method.delivery_tag)

        if consecutive_duplicates >= MAX_DUPLICATES_IN_A_ROW:
            ch.stop_consuming()

    channel.basic_consume(queue=QUEUE_NAME, on_message_callback=callback)
    try:
        channel.start_consuming()
    finally:
        connection.close()

    if not collected:
        print("Новых сообщений в очереди не найдено.")
        return ""

    # Записываем файл строго в каталог DATA/egiazarian_aa/
    ts_str = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    s3_key = f"DATA/{USER_LOGIN}/delivery_{ts_str}.json"

    s3 = _get_s3_client()
    s3.put_object(
        Bucket=S3_BUCKET,
        Key=s3_key,
        Body=json.dumps(collected, ensure_ascii=False).encode("utf-8"),
        ContentType="application/json",
    )
    print(f"Успешно сохранено {len(collected)} записей в s3://{S3_BUCKET}/{s3_key}")
    return s3_key


# ---------- ЗАДАЧА 2: Заливка данных из S3 в ClickHouse ----------
def load_from_s3_to_clickhouse(**context) -> None:
    ti = context["ti"]
    s3_key = ti.xcom_pull(task_ids="extract_rabbit_to_s3")

    if not s3_key:
        print("Путь к файлу S3 не получен (нет новых данных для загрузки).")
        return

    s3 = _get_s3_client()
    obj = s3.get_object(Bucket=S3_BUCKET, Key=s3_key)
    messages = json.loads(obj["Body"].read().decode("utf-8"))

    clickhouse_host = Variable.get("clickhouse_host")
    clickhouse_login = Variable.get("clickhouse_tech_user_account")
    clickhouse_password = Variable.get("clickhouse_tech_user_password")

    with Client(
        host=clickhouse_host,
        user=clickhouse_login,
        password=clickhouse_password,
        database=DB_NAME,
        port=9000,
    ) as client:
        # Получаем уже существующие ключи измерений, чтобы не дублировать справочники
        existing_dates = {r[0] for r in client.execute(f"SELECT date_id FROM {DB_NAME}.dim_date")}
        existing_regions = {r[0] for r in client.execute(f"SELECT region_id FROM {DB_NAME}.dim_region")}
        existing_types = {r[0] for r in client.execute(f"SELECT delivery_type_id FROM {DB_NAME}.dim_delivery_type")}
        existing_routes = {r[0] for r in client.execute(f"SELECT route_id FROM {DB_NAME}.dim_route")}

        dates_rows, regions_rows, types_rows, routes_rows, facts_rows = [], [], [], [], []

        for m in messages:
            event_time = _parse_dt(m.get("event_time"))
            if not event_time:
                continue

            # 1. dim_date
            d = event_time.date()
            date_id = int(d.strftime("%Y%m%d"))
            if date_id not in existing_dates:
                existing_dates.add(date_id)
                dow = d.isoweekday()
                dates_rows.append((
                    date_id, d, d.year, (d.month - 1) // 3 + 1,
                    d.month, d.day, dow, MONTH_NAMES[d.month],
                    1 if dow in (6, 7) else 0, 0
                ))

            # 2. dim_region
            reg_name = m.get("region_name") or "Не указан"
            city = m.get("city")
            zone = m.get("delivery_zone")
            country = m.get("country") or "Россия"
            region_id = _parse_int(m.get("region_id")) or _make_surrogate_id(f"{reg_name}|{city}|{zone}")
            if region_id not in existing_regions:
                existing_regions.add(region_id)
                regions_rows.append((region_id, reg_name, city, zone, country))

            # 3. dim_delivery_type
            type_code = m.get("status_id") or m.get("type_code") or "unknown"
            type_name = m.get("status_name") or m.get("type_name") or type_code
            desc = m.get("carrier_name")
            delivery_type_id = _make_surrogate_id(type_code)
            if delivery_type_id not in existing_types:
                existing_types.add(delivery_type_id)
                types_rows.append((delivery_type_id, type_code, type_name, desc))

            # 4. dim_route
            start_pt = m.get("start_point") or m.get("carrier_id") or "Не указано"
            end_pt = m.get("end_point") or m.get("carrier_name") or "Не указано"
            road_type = m.get("road_type") or m.get("event_type")
            dist_km = _parse_decimal(m.get("distance_km")) or Decimal("0.00")
            route_id = _make_surrogate_id(f"{start_pt}|{end_pt}|{road_type}")
            if route_id not in existing_routes:
                existing_routes.add(route_id)
                routes_rows.append((route_id, start_pt, end_pt, road_type, dist_km))

            # 5. Расчет мер для fact_delivery
            expected_ts = _parse_dt(m.get("expected_delivery_ts"))
            actual_ts = _parse_dt(m.get("actual_delivery_ts"))

            planned_hours = None
            if expected_ts:
                planned_hours = Decimal(str(round(abs((expected_ts - event_time).total_seconds()) / 3600.0, 2)))

            actual_hours = None
            if actual_ts:
                actual_hours = Decimal(str(round(abs((actual_ts - event_time).total_seconds()) / 3600.0, 2)))

            is_on_time = None
            if actual_ts and expected_ts:
                is_on_time = 1 if actual_ts <= expected_ts else 0

            delivery_id = _parse_int(m.get("delivery_id")) or _make_surrogate_id(str(m.get("event_id")))
            order_id = _parse_int(m.get("order_id"))

            facts_rows.append((
                delivery_id,
                order_id,
                route_id,
                date_id,
                region_id,
                delivery_type_id,
                actual_hours,
                planned_hours,
                _parse_decimal(m.get("delivery_cost")),
                is_on_time,
                _parse_decimal(m.get("weight_kg")),
                _parse_decimal(m.get("volume_m3")),
            ))

        # Пакетная вставка в ClickHouse
        if dates_rows:
            client.execute(f"INSERT INTO {DB_NAME}.dim_date VALUES", dates_rows)
        if regions_rows:
            client.execute(f"INSERT INTO {DB_NAME}.dim_region VALUES", regions_rows)
        if types_rows:
            client.execute(f"INSERT INTO {DB_NAME}.dim_delivery_type VALUES", types_rows)
        if routes_rows:
            client.execute(f"INSERT INTO {DB_NAME}.dim_route VALUES", routes_rows)
        if facts_rows:
            client.execute(
                f"""INSERT INTO {DB_NAME}.fact_delivery
                    (delivery_id, order_id, route_id, date_id, region_id, delivery_type_id,
                     actual_duration_hours, planned_duration_hours, delivery_cost,
                     is_on_time, weight_kg, volume_m3) VALUES""",
                facts_rows,
            )

        print(f"Успешно загружено строк в {DB_NAME}.fact_delivery: {len(facts_rows)}")
