S3_COLLECTION = "s3_dad_ksu_datalab"
S3_FILENAME = "dad-ksu-datalab/DATA/piksaev_kiu/*.jsonl"

EVENT_FILTER = "WHERE lower(event_type) = 'receipt_line'"

STG_STRUCTURE = (
    "event_id String, event_type String, event_time String, receipt_id String, "
    "receipt_status String, store_id String, store_name String, store_city String, "
    "store_region String, product_id String, product_name String, product_category String, "
    "product_brand String, customer_id String, customer_segment String, quantity Int32, "
    "gross_amount Float64, discount_amount Float64, net_amount Float64, cost_amount Float64"
)

SQL_TRUNCATE_STG = "TRUNCATE TABLE piksaev_kiu_db.stg_events"

SQL_LOAD_STG = f"""
INSERT INTO piksaev_kiu_db.stg_events
SELECT *
FROM s3(
    {S3_COLLECTION},
    filename = '{S3_FILENAME}',
    format = 'JSONEachRow',
    structure = '{STG_STRUCTURE}'
)
"""

SQL_COUNT_STG = "SELECT count() FROM piksaev_kiu_db.stg_events"

SQL_COUNT_LINES = f"""
SELECT count()
FROM piksaev_kiu_db.stg_events
{EVENT_FILTER}
"""

SQL_TRUNCATE_TARGETS = [
    "TRUNCATE TABLE piksaev_kiu_db.dim_date",
    "TRUNCATE TABLE piksaev_kiu_db.dim_products",
    "TRUNCATE TABLE piksaev_kiu_db.dim_customer",
    "TRUNCATE TABLE piksaev_kiu_db.dim_store",
    "TRUNCATE TABLE piksaev_kiu_db.dim_region",
    "TRUNCATE TABLE piksaev_kiu_db.dim_status",
    "TRUNCATE TABLE piksaev_kiu_db.fact_ecom_orders",
]

SQL_DIMENSIONS = [
    f"""
    INSERT INTO piksaev_kiu_db.dim_date
    SELECT 0 AS date_id, toDate('1970-01-01') AS date, toUInt8(0) AS month, toUInt16(0) AS year
    UNION ALL
    SELECT
        toUInt32(formatDateTime(dt, '%Y%m%d')) AS date_id,
        toDate(dt)                             AS date,
        toUInt8(toMonth(dt))                   AS month,
        toUInt16(toYear(dt))                   AS year
    FROM (
        SELECT DISTINCT toDateTime(parseDateTimeBestEffort(event_time)) AS dt
        FROM piksaev_kiu_db.stg_events
        {EVENT_FILTER}
    )
    ORDER BY date_id
    """,
    f"""
    INSERT INTO piksaev_kiu_db.dim_products
    SELECT
        toUInt32(splitByChar('-', product_id)[2]) AS pid,
        any(product_name)                         AS product_name,
        any(product_category)                     AS category,
        any(product_brand)                        AS brand
    FROM piksaev_kiu_db.stg_events
    {EVENT_FILTER}
    GROUP BY pid
    ORDER BY pid
    """,
    f"""
    INSERT INTO piksaev_kiu_db.dim_customer
    SELECT
        toUInt32(splitByChar('-', customer_id)[2]) AS cid,
        any(customer_id)                           AS customer_name,
        any(customer_segment)                      AS customer_segment
    FROM piksaev_kiu_db.stg_events
    {EVENT_FILTER}
    GROUP BY cid
    ORDER BY cid
    """,
    f"""
    INSERT INTO piksaev_kiu_db.dim_store
    SELECT
        toUInt32(splitByChar('-', store_id)[2]) AS sid,
        any(store_name)                         AS store_name,
        any(store_city)                         AS city,
        any(store_region)                       AS region
    FROM piksaev_kiu_db.stg_events
    {EVENT_FILTER}
    GROUP BY sid
    ORDER BY sid
    """,
    f"""
    INSERT INTO piksaev_kiu_db.dim_region
    SELECT toUInt32(rn) AS region_id, region_name
    FROM (
        SELECT region_name, row_number() OVER (ORDER BY region_name) AS rn
        FROM (
            SELECT DISTINCT store_region AS region_name
            FROM piksaev_kiu_db.stg_events
            {EVENT_FILTER}
        )
    )
    ORDER BY region_id
    """,
    f"""
    INSERT INTO piksaev_kiu_db.dim_status
    SELECT toUInt32(rn) AS status_id, status_name
    FROM (
        SELECT status_name, row_number() OVER (ORDER BY status_name) AS rn
        FROM (
            SELECT DISTINCT receipt_status AS status_name
            FROM piksaev_kiu_db.stg_events
            {EVENT_FILTER}
        )
    )
    ORDER BY status_id
    """,
]

SQL_FACT = f"""
INSERT INTO piksaev_kiu_db.fact_ecom_orders
SELECT
    s.event_id                                          AS event_id,
    toUInt32(splitByChar('-', s.receipt_id)[2])         AS order_id,
    toUInt32(splitByChar('-', s.product_id)[2])         AS product_id,
    toUInt32(splitByChar('-', s.customer_id)[2])        AS customer_id,
    toUInt32(splitByChar('-', s.store_id)[2])           AS store_id,
    r.region_id                                         AS region_id,
    st.status_id                                        AS status_id,
    toUInt32(formatDateTime(s.dt, '%Y%m%d'))            AS date_id,
    toUInt32(0)                                         AS delivery_date_id,
    toUInt32(greatest(s.quantity, 0))                   AS quantity,
    toDecimal64(if(s.quantity > 0, s.gross_amount / s.quantity, 0), 4) AS price,
    toDecimal64(s.gross_amount, 4)                      AS gross_amount,
    toDecimal64(s.net_amount, 4)                        AS net_amount,
    toDecimal64(s.discount_amount, 4)                   AS discount,
    toDecimal64(s.cost_amount, 4)                       AS cost_amount,
    s.dt                                                AS event_time
FROM (
    SELECT *, toDateTime(parseDateTimeBestEffort(event_time)) AS dt
    FROM piksaev_kiu_db.stg_events
    {EVENT_FILTER}
    ORDER BY event_time
    LIMIT 1 BY event_id
) AS s
LEFT JOIN piksaev_kiu_db.dim_region AS r ON toString(r.region_name) = s.store_region
LEFT JOIN piksaev_kiu_db.dim_status AS st ON toString(st.status_name) = s.receipt_status
"""


def rebuild_dimensions_and_fact(client):
    for statement in SQL_TRUNCATE_TARGETS:
        client.execute(statement)
    for statement in SQL_DIMENSIONS:
        client.execute(statement)
    client.execute(SQL_FACT)


def load_s3_to_clickhouse(**context):
    from airflow.models import Variable
    from clickhouse_driver.client import Client

    clickhouse_svc_login = Variable.get("clickhouse_tech_user_account")
    clickhouse_svc_password = Variable.get("clickhouse_tech_user_password")
    clickhouse_host = Variable.get("clickhouse_host", default_var="111.88.243.32")

    with Client(
        host=clickhouse_host,
        user=clickhouse_svc_login,
        password=clickhouse_svc_password,
        database="piksaev_kiu_db",
        port=9000,
    ) as client:
        client.execute(SQL_TRUNCATE_STG)
        client.execute(SQL_LOAD_STG)

        rows_all = client.execute(SQL_COUNT_STG)[0][0]
        rows_lines = client.execute(SQL_COUNT_LINES)[0][0]
        print(f"ClickHouse: в staging {rows_all} строк, из них receipt_line {rows_lines}")

        if rows_all == 0:
            print("ClickHouse: staging пуст, справочники и факт не трогаем")
            return

        if rows_lines == 0:
            raise RuntimeError("в staging нет строк с event_type='receipt_line'")

        rebuild_dimensions_and_fact(client)
        print("ClickHouse: справочники и факт перезалиты из S3")
