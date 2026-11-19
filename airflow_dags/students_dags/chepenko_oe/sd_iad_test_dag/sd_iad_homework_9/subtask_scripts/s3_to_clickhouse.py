
import io
import json
import hashlib
import logging

from datetime import datetime
from decimal import Decimal

import boto3
import pyarrow.parquet as pq
import requests

from subtask_scripts.config import get_connection_settings


LOG = logging.getLogger(__name__)

BUCKET = "dad-ksu-datalab"
S3_ENDPOINT = "https://storage.yandexcloud.net"

CH_DATABASE = "chepenko_oe_db"


def make_key(namespace, value, registry):
    """
    Создает стабильный UInt32 ключ из исходного идентификатора.
    Проверяет коллизии внутри текущего набора данных.
    """

    natural_id = str(value)

    source = f"{namespace}:{natural_id}"

    key = int.from_bytes(
        hashlib.sha256(source.encode("utf-8")).digest()[:4],
        byteorder="big",
    )

    registry_key = (namespace, key)

    previous = registry.get(registry_key)

    if previous is not None and previous != natural_id:
        raise ValueError(
            f"Коллизия ключей {namespace}: "
            f"{previous} и {natural_id}"
        )

    registry[registry_key] = natural_id

    return key


def insert_json_each_row(cfg, table_name, rows):

    if not rows:
        LOG.info("Таблица %s: нет строк для загрузки", table_name)
        return

    url = f"http://{cfg['ch_host']}:{cfg['ch_port']}/"

    query = (
        f"INSERT INTO {CH_DATABASE}.{table_name} "
        f"FORMAT JSONEachRow"
    )

    data = "\n".join(
        json.dumps(
            row,
            ensure_ascii=False,
            default=str,
        )
        for row in rows
    )

    response = requests.post(
        url,
        params={"query": query},
        data=data.encode("utf-8"),
        auth=(
            cfg["ch_user"],
            cfg["ch_password"],
        ),
        timeout=120,
    )

    response.raise_for_status()

    LOG.info(
        "В таблицу %s загружено %s строк",
        table_name,
        len(rows),
    )


def load_s3_to_clickhouse(s3_key=None, **kwargs):

    cfg = get_connection_settings()

    if not s3_key:
        ti = kwargs["ti"]

        s3_key = ti.xcom_pull(
            task_ids="chepenko_oe_rabbit_to_s3"
        )

    if not s3_key:
        raise ValueError("Не получен путь к Parquet-файлу из XCom")

    LOG.info("Начинаем загрузку файла %s", s3_key)

    session = boto3.session.Session()

    s3 = session.client(
        "s3",
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id=cfg["s3_access"],
        aws_secret_access_key=cfg["s3_secret"],
    )

    response = s3.get_object(
        Bucket=BUCKET,
        Key=s3_key,
    )

    parquet_data = response["Body"].read()

    table = pq.read_table(
        io.BytesIO(parquet_data)
    )

    expected_columns = {
        "supply_line_id",
        "supply_id",
        "event_time",
        "supplier_id",
        "supplier_name",
        "supplier_country",
        "product_id",
        "product_name",
        "product_category",
        "product_brand",
        "warehouse_id",
        "warehouse_name",
        "warehouse_region",
        "ordered_qty",
        "received_qty",
        "shortage_qty",
        "received_amount",
        "unit_price",
        "delay_days",
        "event_type",
    }

    actual_columns = set(table.column_names)

    missing_columns = expected_columns - actual_columns

    if missing_columns:
        raise ValueError(
            f"В Parquet отсутствуют колонки: {missing_columns}"
        )

    source_rows = table.to_pylist()

    if not source_rows:
        LOG.info("Parquet-файл пуст")
        return

    registry = {}

    dates = {}
    suppliers = {}
    products = {}
    warehouses = {}
    facts = {}

    for row in source_rows:

        event_time = row["event_time"]

        if not isinstance(event_time, datetime):
            raise ValueError(
                "event_time должен иметь тип datetime"
            )

        date_value = event_time.date()

        date_key = int(date_value.strftime("%Y%m%d"))

        dates[date_key] = {
            "date_key": date_key,
            "date": datetime.combine(
                date_value,
                datetime.min.time(),
            ).strftime("%Y-%m-%d %H:%M:%S"),
        }

        supplier_id = str(row["supplier_id"])

        supplier_key = make_key(
            "supplier",
            supplier_id,
            registry,
        )

        suppliers[supplier_id] = {
            "supplier_key": supplier_key,
            "supplier_id": supplier_id,
            "name": row.get("supplier_name"),
            "country": row.get("supplier_country"),
        }

        product_id = str(row["product_id"])

        product_key = make_key(
            "product",
            product_id,
            registry,
        )

        products[product_id] = {
            "product_key": product_key,
            "product_id": product_id,
            "name": row.get("product_name"),
            "category": row.get("product_category"),
            "brand": row.get("product_brand"),
        }

        warehouse_id = str(row["warehouse_id"])

        warehouse_key = make_key(
            "warehouse",
            warehouse_id,
            registry,
        )

        warehouses[warehouse_id] = {
            "warehouse_key": warehouse_key,
            "warehouse_id": warehouse_id,
            "name": row.get("warehouse_name"),
            "city": None,
        }

        supply_line_id = str(row["supply_line_id"])

        supply_line_key = make_key(
            "supply_line",
            supply_line_id,
            registry,
        )

        facts[supply_line_id] = {
            "supply_line_key": supply_line_key,
            "date_key": date_key,
            "supplier_key": supplier_key,
            "warehouse_key": warehouse_key,
            "product_key": product_key,
            "supply_id": str(row["supply_id"]),
            "ordered_qty": int(row["ordered_qty"]),
            "received_qty": int(row["received_qty"]),
            "shortage_qty": int(row["shortage_qty"]),
            "received_amount": str(
                Decimal(str(row["received_amount"]))
            ),
            "unit_price": str(
                Decimal(str(row["unit_price"]))
            ),
            "delay_days": int(row["delay_days"]),
            "supply_line_id": supply_line_id,
        }

    # Сначала загружаем измерения

    insert_json_each_row(
        cfg,
        "dim_date",
        list(dates.values()),
    )

    insert_json_each_row(
        cfg,
        "dim_supplies",
        list(suppliers.values()),
    )

    insert_json_each_row(
        cfg,
        "dim_product",
        list(products.values()),
    )

    insert_json_each_row(
        cfg,
        "dim_warehouse",
        list(warehouses.values()),
    )

    # Затем загружаем фактовую таблицу

    insert_json_each_row(
        cfg,
        "fact_supply_lines",
        list(facts.values()),
    )

    LOG.info(
        "Загрузка завершена. Исходных строк: %s. "
        "Даты: %s, поставщики: %s, товары: %s, "
        "склады: %s, факты: %s",
        len(source_rows),
        len(dates),
        len(suppliers),
        len(products),
        len(warehouses),
        len(facts),
    )

    return len(source_rows)

