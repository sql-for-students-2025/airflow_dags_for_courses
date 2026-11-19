
import logging
import requests

from subtask_scripts.config import get_connection_settings


LOG = logging.getLogger(__name__)

BUCKET = "dad-ksu-datalab"
S3_ENDPOINT = "https://storage.yandexcloud.net"
CH_DATABASE = "chepenko_oe_db"

STAGE_TABLES = [
    "dim_date_stage",
    "dim_supplies_stage",
    "dim_product_stage",
    "dim_warehouse_stage",
]


def execute_query(cfg, query, s3_params=None):
    """Выполняет SQL-запрос в ClickHouse через HTTP."""

    url = f"http://{cfg['ch_host']}:{cfg['ch_port']}/"

    params = {"query": query}

    if s3_params:
        params.update({
            "param_s3_url": s3_params["url"],
            "param_s3_access": s3_params["access"],
            "param_s3_secret": s3_params["secret"],
        })

    response = requests.post(
        url,
        params=params,
        auth=(cfg["ch_user"], cfg["ch_password"]),
        timeout=300,
    )

    if not response.ok:
        LOG.error(
            "ClickHouse HTTP error %s: %s",
            response.status_code,
            response.text
        )
        response.raise_for_status()

    return response.text.strip()


def make_s3_url(s3_key):
    if s3_key.startswith("http://") or s3_key.startswith("https://"):
        return s3_key

    return f"{S3_ENDPOINT}/{BUCKET}/{s3_key.lstrip('/')}"


def check_duplicates(cfg, table, natural_key, surrogate_key):
    """Проверяет дубликаты в staging и пишет результат в лог."""

    query = f"""
        SELECT
            count(),
            uniqExact({natural_key}),
            uniqExact({surrogate_key})
        FROM {CH_DATABASE}.{table}
        FORMAT TabSeparated
    """

    result = execute_query(cfg, query)
    total, unique_natural, unique_surrogate = map(
        int, result.split("\t")
    )

    LOG.info(
        "%s: строк=%s, уникальных натуральных ключей=%s, "
        "уникальных суррогатных ключей=%s",
        table,
        total,
        unique_natural,
        unique_surrogate,
    )

    if total == 0:
        raise ValueError(f"{table}: staging-таблица пустая")

    if total != unique_natural or total != unique_surrogate:
        raise ValueError(
            f"{table}: обнаружены дубликаты или коллизии ключей"
        )

    LOG.info("%s: проверка дубликатов пройдена", table)


def load_s3_to_clickhouse(s3_key=None, s3_path=None, **kwargs):
    """
    S3 -> staging -> проверки -> EXCHANGE -> загрузка фактов -> TRUNCATE.
    """

    cfg = get_connection_settings()

    if not s3_key:
        s3_key = s3_path

    if not s3_key:
        ti = kwargs.get("ti")

        if ti:
            s3_key = ti.xcom_pull(
                task_ids="chepenko_oe_rabbit_to_s3"
            )

    if not s3_key:
        LOG.info("Новых данных нет")
        return

    s3_params = {
        "url": make_s3_url(s3_key),
        "access": cfg["s3_access"],
        "secret": cfg["s3_secret"],
    }

    source = """
        s3(
            {s3_url:String},
            {s3_access:String},
            {s3_secret:String},
            'Parquet'
        )
    """

    LOG.info("Начинаем обработку файла: %s", s3_key)

    # 1. Проверяем исходный файл.

    source_count = execute_query(
        cfg,
        f"SELECT count() FROM {source} FORMAT TabSeparated",
        s3_params,
    )

    if int(source_count) == 0:
        LOG.info("Parquet-файл пуст. Загрузка пропущена.")
        return

    LOG.info("Исходных строк в Parquet: %s", source_count)

    # 2. Очищаем staging перед загрузкой.

    for table in STAGE_TABLES:
        execute_query(
            cfg,
            f"TRUNCATE TABLE {CH_DATABASE}.{table}"
        )

    # 3. Формируем справочники.

    queries = {
        "dim_date_stage": f"""
            INSERT INTO {CH_DATABASE}.dim_date_stage
                (date_key, date)
            SELECT
                date_key,
                toDateTime(date_value)
            FROM
            (
                SELECT
                    toUInt32(date_key) AS date_key,
                    toDate(date) AS date_value
                FROM {CH_DATABASE}.dim_date

                UNION ALL

                SELECT
                    toYYYYMMDD(toDate(event_time)) AS date_key,
                    toDate(event_time) AS date_value
                FROM {source}
            )
            GROUP BY date_key, date_value
        """,

        "dim_supplies_stage": f"""
            INSERT INTO {CH_DATABASE}.dim_supplies_stage
                (supplier_key, supplier_id, name, country)
            SELECT
                toUInt32(conv(
                    substring(
                        hex(SHA256(concat('supplier:', supplier_id))),
                        1, 8
                    ), 16, 10
                )),
                supplier_id,
                any(name),
                any(country)
            FROM
            (
                SELECT
                    toString(supplier_id) AS supplier_id,
                    name,
                    country
                FROM {CH_DATABASE}.dim_supplies

                UNION ALL

                SELECT
                    toString(supplier_id) AS supplier_id,
                    supplier_name AS name,
                    supplier_country AS country
                FROM {source}
            )
            GROUP BY supplier_id
        """,

        "dim_product_stage": f"""
            INSERT INTO {CH_DATABASE}.dim_product_stage
                (product_key, product_id, name, category, brand)
            SELECT
                toUInt32(conv(
                    substring(
                        hex(SHA256(concat('product:', product_id))),
                        1, 8
                    ), 16, 10
                )),
                product_id,
                any(name),
                any(category),
                any(brand)
            FROM
            (
                SELECT
                    toString(product_id) AS product_id,
                    name,
                    category,
                    brand
                FROM {CH_DATABASE}.dim_product

                UNION ALL

                SELECT
                    toString(product_id) AS product_id,
                    product_name AS name,
                    product_category AS category,
                    product_brand AS brand
                FROM {source}
            )
            GROUP BY product_id
        """,

        "dim_warehouse_stage": f"""
            INSERT INTO {CH_DATABASE}.dim_warehouse_stage
                (warehouse_key, warehouse_id, name, city)
            SELECT
                toUInt32(conv(
                    substring(
                        hex(SHA256(concat('warehouse:', warehouse_id))),
                        1, 8
                    ), 16, 10
                )),
                warehouse_id,
                any(name),
                any(city)
            FROM
            (
                SELECT
                    toString(warehouse_id) AS warehouse_id,
                    name,
                    city
                FROM {CH_DATABASE}.dim_warehouse

                UNION ALL

                SELECT
                    toString(warehouse_id) AS warehouse_id,
                    warehouse_name AS name,
                    CAST(NULL, 'Nullable(String)') AS city
                FROM {source}
            )
            GROUP BY warehouse_id
        """,
    }

    # 4. Загружаем данные в staging.

    for table, query in queries.items():
        execute_query(cfg, query, s3_params)
        LOG.info("Загрузка в %s завершена", table)

    # 5. Проверяем справочники.

    checks = [
        ("dim_date_stage", "date_key", "date_key"),
        ("dim_supplies_stage", "supplier_id", "supplier_key"),
        ("dim_product_stage", "product_id", "product_key"),
        ("dim_warehouse_stage", "warehouse_id", "warehouse_key"),
    ]

    for table, natural_key, surrogate_key in checks:
        check_duplicates(
            cfg,
            table,
            natural_key,
            surrogate_key,
        )

    # 6. Подмена основных таблиц.

    exchanges = [
        ("dim_date", "dim_date_stage"),
        ("dim_supplies", "dim_supplies_stage"),
        ("dim_product", "dim_product_stage"),
        ("dim_warehouse", "dim_warehouse_stage"),
    ]

    for production, stage in exchanges:
        query = f"""
            EXCHANGE TABLES
                {CH_DATABASE}.{production}
            AND
                {CH_DATABASE}.{stage}
        """

        execute_query(cfg, query)
        LOG.info(
            "Выполнена подмена %s <-> %s",
            production,
            stage,
        )

    # 7. Загружаем фактовую таблицу.

    fact_query = f"""
        INSERT INTO {CH_DATABASE}.fact_supply_lines
        (
            supply_line_key,
            date_key,
            supplier_key,
            warehouse_key,
            product_key,
            supply_id,
            ordered_qty,
            received_qty,
            shortage_qty,
            received_amount,
            unit_price,
            delay_days,
            supply_line_id
        )
        SELECT
            toUInt32(conv(
                substring(
                    hex(SHA256(concat('supply_line:', src.supply_line_id))),
                    1, 8
                ), 16, 10
            )),
            toYYYYMMDD(toDate(src.event_time)),
            toUInt32(conv(
                substring(
                    hex(SHA256(concat('supplier:', src.supplier_id))),
                    1, 8
                ), 16, 10
            )),
            toUInt32(conv(
                substring(
                    hex(SHA256(concat('warehouse:', src.warehouse_id))),
                    1, 8
                ), 16, 10
            )),
            toUInt32(conv(
                substring(
                    hex(SHA256(concat('product:', src.product_id))),
                    1, 8
                ), 16, 10
            )),
            src.supply_id,
            toInt32(src.ordered_qty),
            toInt32(src.received_qty),
            toInt32(src.shortage_qty),
            toDecimal64(src.received_amount, 2),
            toDecimal64(src.unit_price, 2),
            toInt32(src.delay_days),
            src.supply_line_id
        FROM
        (
            SELECT
                supply_line_id,
                any(toString(supply_id)) AS supply_id,
                any(event_time) AS event_time,
                any(toString(supplier_id)) AS supplier_id,
                any(toString(product_id)) AS product_id,
                any(toString(warehouse_id)) AS warehouse_id,
                any(ordered_qty) AS ordered_qty,
                any(received_qty) AS received_qty,
                any(shortage_qty) AS shortage_qty,
                any(received_amount) AS received_amount,
                any(unit_price) AS unit_price,
                any(delay_days) AS delay_days
            FROM {source}
            GROUP BY supply_line_id
        ) AS src
        LEFT ANTI JOIN
        (
            SELECT supply_line_id
            FROM {CH_DATABASE}.fact_supply_lines
            WHERE supply_line_id IS NOT NULL
        ) AS old
        USING (supply_line_id)
    """

    execute_query(cfg, fact_query, s3_params)

    LOG.info("Загрузка fact_supply_lines завершена")

    # 8. Очищаем staging после подмены.

    for table in STAGE_TABLES:
        execute_query(
            cfg,
            f"TRUNCATE TABLE {CH_DATABASE}.{table}"
        )
        LOG.info("Staging-таблица %s очищена", table)

    LOG.info("ДЗ №10: загрузка успешно завершена")

    return int(source_count)
