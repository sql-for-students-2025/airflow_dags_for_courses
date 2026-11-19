
import logging
import requests

from subtask_scripts.config import get_connection_settings


LOG = logging.getLogger(__name__)

BUCKET = "dad-ksu-datalab"
S3_ENDPOINT = "https://storage.yandexcloud.net"
CH_DATABASE = "chepenko_oe_db"

# Источник данных: Parquet-файл в S3. Параметры подставляются ClickHouse
# через HTTP query parameters (param_s3_url и т.д.), а не форматированием строки.
SOURCE = """
    s3(
        {s3_url:String},
        {s3_access:String},
        {s3_secret:String},
        'Parquet'
    )
"""


def key_expr(prefix, column):
    """Суррогатный ключ UInt32 из натурального ключа (одинаков для dim и fact).
    Формула не меняется, чтобы ключи совпадали с уже загруженными данными."""
    return (
        f"toUInt32(conv(substring("
        f"hex(SHA256(concat('{prefix}:', toString({column})))), 1, 8), 16, 10))"
    )


# Описание справочников:
#   dim         — боевая таблица
#   stage       — стейджовая таблица (создаётся по структуре dim)
#   natural     — натуральный ключ
#   surrogate   — суррогатный ключ
#   check_by    — по каким колонкам проверяем уникальность
#   query       — SELECT, который собирает полный новый снимок справочника:
#                 старые строки из dim + новые из файла. При конфликте
#                 побеждают данные из файла (prio = 1), но NULL из файла
#                 не затирает уже известное значение.
DIMENSIONS = [
    {
        "dim": "dim_date",
        "stage": "dim_date_stage",
        "columns": "date_key, date",
        "check": ("date_key", "date_key"),
        "query": f"""
            SELECT
                date_key,
                any(date_value)
            FROM
            (
                SELECT
                    toUInt32(date_key) AS date_key,
                    toDate(date) AS date_value
                FROM {CH_DATABASE}.dim_date

                UNION ALL

                SELECT
                    toUInt32(toYYYYMMDD(toDate(event_time))) AS date_key,
                    toDate(event_time) AS date_value
                FROM {SOURCE}
            )
            GROUP BY date_key
        """,
    },
    {
        "dim": "dim_supplies",
        "stage": "dim_supplies_stage",
        "columns": "supplier_key, supplier_id, name, country",
        "check": ("supplier_id", "supplier_key"),
        "query": f"""
            SELECT
                {key_expr('supplier', 'supplier_id')},
                supplier_id,
                argMaxIf(name, prio, name IS NOT NULL),
                argMaxIf(country, prio, country IS NOT NULL)
            FROM
            (
                SELECT
                    toString(supplier_id) AS supplier_id,
                    name,
                    country,
                    0 AS prio
                FROM {CH_DATABASE}.dim_supplies

                UNION ALL

                SELECT
                    toString(supplier_id),
                    supplier_name,
                    supplier_country,
                    1
                FROM {SOURCE}
            )
            GROUP BY supplier_id
        """,
    },
    {
        "dim": "dim_product",
        "stage": "dim_product_stage",
        "columns": "product_key, product_id, name, category, brand",
        "check": ("product_id", "product_key"),
        "query": f"""
            SELECT
                {key_expr('product', 'product_id')},
                product_id,
                argMaxIf(name, prio, name IS NOT NULL),
                argMaxIf(category, prio, category IS NOT NULL),
                argMaxIf(brand, prio, brand IS NOT NULL)
            FROM
            (
                SELECT
                    toString(product_id) AS product_id,
                    name,
                    category,
                    brand,
                    0 AS prio
                FROM {CH_DATABASE}.dim_product

                UNION ALL

                SELECT
                    toString(product_id),
                    product_name,
                    product_category,
                    product_brand,
                    1
                FROM {SOURCE}
            )
            GROUP BY product_id
        """,
    },
    {
        "dim": "dim_warehouse",
        "stage": "dim_warehouse_stage",
        "columns": "warehouse_key, warehouse_id, name, city",
        "check": ("warehouse_id", "warehouse_key"),
        "query": f"""
            SELECT
                {key_expr('warehouse', 'warehouse_id')},
                warehouse_id,
                argMaxIf(name, prio, name IS NOT NULL),
                argMaxIf(city, prio, city IS NOT NULL)
            FROM
            (
                SELECT
                    toString(warehouse_id) AS warehouse_id,
                    name,
                    city,
                    0 AS prio
                FROM {CH_DATABASE}.dim_warehouse

                UNION ALL

                SELECT
                    toString(warehouse_id),
                    warehouse_name,
                    CAST(NULL, 'Nullable(String)'),
                    1
                FROM {SOURCE}
            )
            GROUP BY warehouse_id
        """,
    },
]


def execute_query(cfg, query, s3_params=None):
    """Выполняет SQL-запрос в ClickHouse через HTTP.

    Текст запроса отправляется в теле POST (длинные запросы не упираются
    в лимит длины URL), параметры s3 — через param_*.
    """

    url = f"http://{cfg['ch_host']}:{cfg['ch_port']}/"

    params = {}
    if s3_params:
        params.update({
            "param_s3_url": s3_params["url"],
            "param_s3_access": s3_params["access"],
            "param_s3_secret": s3_params["secret"],
        })

    response = requests.post(
        url,
        params=params,
        data=query.encode("utf-8"),
        auth=(cfg["ch_user"], cfg["ch_password"]),
        timeout=300,
    )

    if not response.ok:
        LOG.error(
            "ClickHouse HTTP error %s: %s",
            response.status_code,
            response.text,
        )
        response.raise_for_status()

    return response.text.strip()


def make_s3_url(s3_key):
    if s3_key.startswith(("http://", "https://")):
        return s3_key

    return f"{S3_ENDPOINT}/{BUCKET}/{s3_key.lstrip('/')}"


def resolve_s3_key(s3_key, s3_path, kwargs):
    """Определяет путь к файлу. Jinja-шаблон с пустым XCom рендерится
    в строку 'None' — её тоже считаем отсутствием данных."""

    for candidate in (s3_key, s3_path):
        if candidate and str(candidate).strip() not in ("None", "null", ""):
            return str(candidate).strip()

    ti = kwargs.get("ti")
    if ti:
        value = ti.xcom_pull(task_ids="chepenko_oe_rabbit_to_s3")
        if value and str(value).strip() not in ("None", "null", ""):
            return str(value).strip()

    return None


def truncate_stage(cfg, stage):
    """Stage-таблицы уже созданы в ClickHouse — только очищаем их."""

    execute_query(cfg, f"TRUNCATE TABLE {CH_DATABASE}.{stage}")
    LOG.info("Stage-таблица %s очищена", stage)


def swap_tables(cfg, dim, stage):
    """Атомарно меняет местами dim и stage (база на движке Atomic)."""

    execute_query(
        cfg,
        f"EXCHANGE TABLES {CH_DATABASE}.{dim} AND {CH_DATABASE}.{stage}",
    )
    LOG.info("Выполнена подмена %s <-> %s", dim, stage)


def count_rows(cfg, table):
    return int(execute_query(
        cfg,
        f"SELECT count() FROM {CH_DATABASE}.{table} FORMAT TabSeparated",
    ))


def check_stage(cfg, dim, stage, natural_key, surrogate_key):
    """Проверяет stage перед подменой:
    - не пустая;
    - нет дублей натурального ключа и коллизий суррогатного;
    - в stage не меньше строк, чем в текущей dim (ничего не потеряли).
    """

    result = execute_query(cfg, f"""
        SELECT
            count(),
            uniqExact({natural_key}),
            uniqExact({surrogate_key})
        FROM {CH_DATABASE}.{stage}
        FORMAT TabSeparated
    """)
    total, unique_natural, unique_surrogate = map(int, result.split("\t"))
    dim_total = count_rows(cfg, dim)

    LOG.info(
        "%s: строк=%s (в %s было %s), уникальных натуральных=%s, "
        "уникальных суррогатных=%s",
        stage, total, dim, dim_total, unique_natural, unique_surrogate,
    )

    if total == 0:
        raise ValueError(f"{stage}: stage-таблица пустая")

    if total != unique_natural or total != unique_surrogate:
        raise ValueError(f"{stage}: обнаружены дубликаты или коллизии ключей")

    if total < dim_total:
        raise ValueError(
            f"{stage}: строк меньше, чем в {dim} ({total} < {dim_total})"
        )


def load_s3_to_clickhouse(s3_key=None, s3_path=None, **kwargs):
    """
    S3 -> stage -> проверки -> EXCHANGE stage <-> dim -> факты -> очистка stage.
    """

    cfg = get_connection_settings()

    s3_key = resolve_s3_key(s3_key, s3_path, kwargs)
    if not s3_key:
        LOG.info("Новых данных нет")
        return None

    s3_params = {
        "url": make_s3_url(s3_key),
        "access": cfg["s3_access"],
        "secret": cfg["s3_secret"],
    }

    LOG.info("Начинаем обработку файла: %s", s3_key)

    # 1. Проверяем исходный файл.
    source_count = int(execute_query(
        cfg,
        f"SELECT count() FROM {SOURCE} FORMAT TabSeparated",
        s3_params,
    ))
    if source_count == 0:
        LOG.info("Parquet-файл пуст. Загрузка пропущена.")
        return 0

    LOG.info("Исходных строк в Parquet: %s", source_count)

    # 2. Очищаем stage-таблицы и наполняем их полным новым снимком
    #    справочника (старые данные из dim + новые из файла).
    for d in DIMENSIONS:
        truncate_stage(cfg, d["stage"])
        execute_query(
            cfg,
            f"INSERT INTO {CH_DATABASE}.{d['stage']} ({d['columns']}) "
            f"{d['query']}",
            s3_params,
        )
        LOG.info("Загрузка в %s завершена", d["stage"])

    # 3. Проверяем ВСЕ stage до первой подмены: если хоть одна проверка
    #    не прошла, боевые dim-таблицы остаются нетронутыми.
    for d in DIMENSIONS:
        natural_key, surrogate_key = d["check"]
        check_stage(cfg, d["dim"], d["stage"], natural_key, surrogate_key)

    # 4. Подмена: dim получает новые данные, stage — старые.
    for d in DIMENSIONS:
        swap_tables(cfg, d["dim"], d["stage"])

    # 5. Загружаем факты (только новые supply_line_id).
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
            {key_expr('supply_line', 'src.supply_line_id')},
            toUInt32(toYYYYMMDD(toDate(src.event_time))),
            {key_expr('supplier', 'src.supplier_id')},
            {key_expr('warehouse', 'src.warehouse_id')},
            {key_expr('product', 'src.product_id')},
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
            FROM {SOURCE}
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

    # 6. После подмены в stage лежат старые версии справочников — очищаем.
    for d in DIMENSIONS:
        truncate_stage(cfg, d["stage"])

    LOG.info("Загрузка успешно завершена")
    return source_count
