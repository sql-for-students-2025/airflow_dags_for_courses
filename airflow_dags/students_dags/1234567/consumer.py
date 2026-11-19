import json
import os
import re
import time
from datetime import datetime

import pika
import psycopg2
from dotenv import load_dotenv

print("=== СТАРТ НЕПРЕРЫВНОГО КОНСЬЮМЕРА (БЕЗ УДАЛЕНИЯ ИЗ ОЧЕРЕДИ) ===")
load_dotenv()

# ---------- Настройки подключений ----------
RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "")
RABBITMQ_PORT = int(os.getenv("RABBITMQ_PORT", "5672"))
RABBITMQ_USER = os.getenv("RABBITMQ_USER", "")
RABBITMQ_PASS = os.getenv("RABBITMQ_PASS", "")

PG_HOST = os.getenv("PG_HOST", "")
PG_PORT = int(os.getenv("PG_PORT", "6432"))
PG_USER = os.getenv("PG_USER", "")
PG_PASSWORD = os.getenv("PG_PASSWORD", "")
PG_DB_NAME = os.getenv("PG_DB_NAME", "teacher")

QUEUE = "delivery.queue"  # <-- Убедитесь, что здесь ваше точное имя очереди!
SCHEMA = "egiazarian_aa_sch"

MONTH_NAMES = {
    1: "January", 2: "February", 3: "March", 4: "April",
    5: "May", 6: "June", 7: "July", 8: "August",
    9: "September", 10: "October", 11: "November", 12: "December"
}


# ---------- Функции парсинга ----------
def parse_dt(value: str) -> datetime | None:
    if not value:
        return None
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def parse_int(value) -> int | None:
    if value is None:
        return None
    if isinstance(value, int):
        return value
    digits = re.sub(r"\D", "", str(value))
    return int(digits) if digits else None


def parse_float(value) -> float | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    cleaned = re.sub(r"[^\d.,-]", "", str(value)).replace(",", ".")
    try:
        return float(cleaned) if cleaned else None
    except ValueError:
        return None


# ---------- Работа с измерениями (DIM) ----------
def get_date_id(cur, dt: datetime) -> int:
    d = dt.date()
    cur.execute(f"SELECT date_id FROM {SCHEMA}.dim_date WHERE calendar_date = %s", (d,))
    row = cur.fetchone()
    if row:
        return row[0]

    day_of_week = d.isoweekday()
    is_weekend = day_of_week in (6, 7)
    is_holiday = False

    cur.execute(
        f"""INSERT INTO {SCHEMA}.dim_date 
            (calendar_date, year, quarter, month, day, day_of_week, month_name, is_weekend, is_holiday)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING date_id""",
        (d, d.year, (d.month - 1) // 3 + 1, d.month, d.day, day_of_week, MONTH_NAMES[d.month], is_weekend, is_holiday)
    )
    return cur.fetchone()[0]


def get_region_id(cur, region_name: str, city: str, delivery_zone: str, country: str) -> int:
    region_name = region_name or "Не указан"
    country = country or "Россия"

    cur.execute(
        f"""SELECT region_id FROM {SCHEMA}.dim_region 
            WHERE region_name = %s 
              AND city IS NOT DISTINCT FROM %s 
              AND delivery_zone IS NOT DISTINCT FROM %s""",
        (region_name, city, delivery_zone)
    )
    row = cur.fetchone()
    if row:
        return row[0]

    cur.execute(
        f"""INSERT INTO {SCHEMA}.dim_region (region_name, city, delivery_zone, country)
            VALUES (%s, %s, %s, %s)
            RETURNING region_id""",
        (region_name, city, delivery_zone, country)
    )
    return cur.fetchone()[0]


def get_delivery_type_id(cur, type_code: str, type_name: str = None, description: str = None) -> int:
    type_code = type_code or "unknown"
    type_name = type_name or type_code

    cur.execute(f"SELECT delivery_type_id FROM {SCHEMA}.dim_delivery_type WHERE type_code = %s", (type_code,))
    row = cur.fetchone()
    if row:
        return row[0]

    print(f"  >>> Создаем новый тип/статус доставки: {type_code} ({type_name})")
    cur.execute(
        f"""INSERT INTO {SCHEMA}.dim_delivery_type (type_code, type_name, description)
            VALUES (%s, %s, %s)
            RETURNING delivery_type_id""",
        (type_code, type_name, description)
    )
    return cur.fetchone()[0]


def get_route_id(cur, start_point: str, end_point: str, road_type: str, distance_km: float) -> int:
    start_point = start_point or "Не указано"
    end_point = end_point or "Не указано"
    distance_km = distance_km if distance_km is not None else 0.0

    cur.execute(
        f"""SELECT route_id FROM {SCHEMA}.dim_route 
            WHERE start_point = %s 
              AND end_point = %s 
              AND road_type IS NOT DISTINCT FROM %s""",
        (start_point, end_point, road_type)
    )
    row = cur.fetchone()
    if row:
        return row[0]

    cur.execute(
        f"""INSERT INTO {SCHEMA}.dim_route (start_point, end_point, road_type, distance_km)
            VALUES (%s, %s, %s, %s)
            RETURNING route_id""",
        (start_point, end_point, road_type, distance_km)
    )
    return cur.fetchone()[0]


# ---------- Запись одного сообщения в PostgreSQL ----------
def save_single_message(conn, cur, m: dict) -> bool:
    event_time = parse_dt(m.get("event_time"))
    if event_time is None:
        return False

    expected_ts = parse_dt(m.get("expected_delivery_ts"))
    actual_ts = parse_dt(m.get("actual_delivery_ts"))

    planned_hours = None
    if expected_ts and event_time:
        planned_hours = round(abs((expected_ts - event_time).total_seconds()) / 3600.0, 2)

    actual_hours = None
    if actual_ts and event_time:
        actual_hours = round(abs((actual_ts - event_time).total_seconds()) / 3600.0, 2)

    is_on_time = None
    if actual_ts and expected_ts:
        is_on_time = actual_ts <= expected_ts

    date_id = get_date_id(cur, event_time)
    region_id = get_region_id(
        cur,
        m.get("region_name") or m.get("region"),
        m.get("city"),
        m.get("delivery_zone"),
        m.get("country")
    )
    delivery_type_id = get_delivery_type_id(
        cur,
        m.get("status_id") or m.get("type_code"),
        m.get("status_name") or m.get("type_name"),
        m.get("carrier_name")
    )
    route_id = get_route_id(
        cur,
        m.get("start_point") or m.get("carrier_id"),
        m.get("end_point") or m.get("carrier_name"),
        m.get("road_type") or m.get("event_type"),
        parse_float(m.get("distance_km")) or 0.0
    )

    cur.execute(
        f"""INSERT INTO {SCHEMA}.fact_delivery
            (order_id, route_id, date_id, region_id, delivery_type_id,
             actual_duration_hours, planned_duration_hours, delivery_cost,
             is_on_time, weight_kg, volume_m3)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
        (
            parse_int(m.get("order_id")) or 0,
            route_id,
            date_id,
            region_id,
            delivery_type_id,
            actual_hours,
            planned_hours,
            parse_float(m.get("delivery_cost")),
            is_on_time,
            parse_float(m.get("weight_kg")),
            parse_float(m.get("volume_m3"))
        )
    )
    conn.commit()
    return True


# ---------- Постоянное подключение к PostgreSQL и RabbitMQ ----------
print("Подключаемся к PostgreSQL...")
pg_conn = psycopg2.connect(
    database=PG_DB_NAME, user=PG_USER, password=PG_PASSWORD, port=PG_PORT, host=PG_HOST, connect_timeout=10
)
pg_cur = pg_conn.cursor()
print("PostgreSQL подключен!")

credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS)
parameters = pika.ConnectionParameters(
    host=RABBITMQ_HOST,
    port=RABBITMQ_PORT,
    credentials=credentials,
    heartbeat=600,
    blocked_connection_timeout=300
)

print("Подключаемся к RabbitMQ...")
connection = pika.BlockingConnection(parameters)
print("RabbitMQ подключен! Начинаем непрерывное чтение (для остановки нажмите Ctrl+C)...\n")

seen_ids = set()
total_inserted = 0

try:
    while True:
        # Открываем канал на каждый проход по очереди:
        # все прочитанные за проход сообщения автоматически вернутся в очередь при закрытии канала!
        channel = connection.channel()
        new_in_pass = 0

        while True:
            # basic_get(auto_ack=False) берет следующее неподтвержденное сообщение в рамках этого канала
            method_frame, properties, body = channel.basic_get(queue=QUEUE, auto_ack=False)

            # Если дошли до конца очереди (все текущие сообщения просмотрены) — выходим из внутреннего цикла
            if method_frame is None:
                break

            try:
                message = json.loads(body.decode("utf-8"))
                msg_id = message.get("event_id")

                if msg_id not in seen_ids:
                    seen_ids.add(msg_id)
                    if save_single_message(pg_conn, pg_cur, message):
                        total_inserted += 1
                        new_in_pass += 1
                        print(f"  [+] Записано в PostgreSQL (всего: {total_inserted}) | event_id: {msg_id}")
            except json.JSONDecodeError:
                print("  [-] Получено сообщение не в формате JSON")
            except Exception as e:
                pg_conn.rollback()
                print(f"  [!] Ошибка при записи в БД: {e}")

        # Закрываем канал: RabbitMQ мгновенно возвращает ВСЕ просмотренные сообщения обратно в очередь нетронутыми!
        channel.close()

        if new_in_pass == 0:
            # Ждем 3 секунды перед проверкой новых сообщений от продюсера
            time.sleep(3)

except KeyboardInterrupt:
    print("\nОстановка консьюмера пользователем (Ctrl+C)...")
finally:
    if connection.is_open:
        connection.close()
    pg_cur.close()
    pg_conn.close()
    print(f"Все соединения закрыты. Итого записано строк: {total_inserted}")
