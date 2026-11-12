S3_COLLECTION = "s3_dad_ksu_datalab"
S3_FILENAME = "dad-ksu-datalab/DATA/piksaev_kiu/*.jsonl"

ONLY_LINES = "where lower(event_type) = 'receipt_line'"

STG_COLUMNS = (
    "event_id String, event_type String, event_time String, receipt_id String, "
    "receipt_status String, store_id String, store_name String, store_city String, "
    "store_region String, product_id String, product_name String, product_category String, "
    "product_brand String, customer_id String, customer_segment String, quantity Int32, "
    "gross_amount Float64, discount_amount Float64, net_amount Float64, cost_amount Float64"
)

DIMS = [
    "dim_date",
    "dim_products",
    "dim_customer",
    "dim_store",
    "dim_region",
    "dim_status",
]

LOAD_STG = f"""
insert into piksaev_kiu_db.stg_events
select *
from s3(
    {S3_COLLECTION},
    filename = '{S3_FILENAME}',
    format = 'JSONEachRow',
    structure = '{STG_COLUMNS}'
)
"""

DIM_SQL = {
    "dim_date": f"""
        insert into piksaev_kiu_db.dim_date_stage
        select 0, toDate('1970-01-01'), toUInt8(0), toUInt16(0)
        union all
        select
            toUInt32(formatDateTime(dt, '%Y%m%d')),
            toDate(dt),
            toUInt8(toMonth(dt)),
            toUInt16(toYear(dt))
        from (
            select distinct toDateTime(parseDateTimeBestEffort(event_time)) as dt
            from piksaev_kiu_db.stg_events
            {ONLY_LINES}
        )
        order by 1
    """,
    "dim_products": f"""
        insert into piksaev_kiu_db.dim_products_stage
        select
            toUInt32(splitByChar('-', product_id)[2]),
            any(product_name),
            any(product_category),
            any(product_brand)
        from piksaev_kiu_db.stg_events
        {ONLY_LINES}
        group by 1
        order by 1
    """,
    "dim_customer": f"""
        insert into piksaev_kiu_db.dim_customer_stage
        select
            toUInt32(splitByChar('-', customer_id)[2]),
            any(customer_id),
            any(customer_segment)
        from piksaev_kiu_db.stg_events
        {ONLY_LINES}
        group by 1
        order by 1
    """,
    "dim_store": f"""
        insert into piksaev_kiu_db.dim_store_stage
        select
            toUInt32(splitByChar('-', store_id)[2]),
            any(store_name),
            any(store_city),
            any(store_region)
        from piksaev_kiu_db.stg_events
        {ONLY_LINES}
        group by 1
        order by 1
    """,
    "dim_region": f"""
        insert into piksaev_kiu_db.dim_region_stage
        select toUInt32(rn), region_name
        from (
            select region_name, row_number() over (order by region_name) as rn
            from (
                select distinct store_region as region_name
                from piksaev_kiu_db.stg_events
                {ONLY_LINES}
            )
        )
        order by 1
    """,
    "dim_status": f"""
        insert into piksaev_kiu_db.dim_status_stage
        select toUInt32(rn), status_name
        from (
            select status_name, row_number() over (order by status_name) as rn
            from (
                select distinct receipt_status as status_name
                from piksaev_kiu_db.stg_events
                {ONLY_LINES}
            )
        )
        order by 1
    """,
}

FACT_SQL = f"""
insert into piksaev_kiu_db.fact_ecom_orders
select
    s.event_id,
    toUInt32(splitByChar('-', s.receipt_id)[2]),
    toUInt32(splitByChar('-', s.product_id)[2]),
    toUInt32(splitByChar('-', s.customer_id)[2]),
    toUInt32(splitByChar('-', s.store_id)[2]),
    r.region_id,
    st.status_id,
    toUInt32(formatDateTime(s.dt, '%Y%m%d')),
    toUInt32(0),
    toUInt32(greatest(s.quantity, 0)),
    toDecimal64(if(s.quantity > 0, s.gross_amount / s.quantity, 0), 4),
    toDecimal64(s.gross_amount, 4),
    toDecimal64(s.net_amount, 4),
    toDecimal64(s.discount_amount, 4),
    toDecimal64(s.cost_amount, 4),
    s.dt
from (
    select *, toDateTime(parseDateTimeBestEffort(event_time)) as dt
    from piksaev_kiu_db.stg_events
    {ONLY_LINES}
    order by event_time
    limit 1 by event_id
) as s
left join piksaev_kiu_db.dim_region as r on toString(r.region_name) = s.store_region
left join piksaev_kiu_db.dim_status as st on toString(st.status_name) = s.receipt_status
"""


def rebuild_dimensions(client):
    for dim in DIMS:
        stage = dim + "_stage"
        client.execute(f"truncate table piksaev_kiu_db.{stage}")
        client.execute(DIM_SQL[dim])
        client.execute(f"exchange tables piksaev_kiu_db.{dim} and piksaev_kiu_db.{stage}")
        client.execute(f"truncate table piksaev_kiu_db.{stage}")
        rows = client.execute(f"select count() from piksaev_kiu_db.{dim}")[0][0]
        print(f"{dim}: {rows} строк")


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
        for dim in DIMS:
            client.execute(
                f"create table if not exists piksaev_kiu_db.{dim}_stage as piksaev_kiu_db.{dim}"
            )

        client.execute("truncate table piksaev_kiu_db.stg_events")
        client.execute(LOAD_STG)

        total = client.execute("select count() from piksaev_kiu_db.stg_events")[0][0]
        lines = client.execute(
            f"select count() from piksaev_kiu_db.stg_events {ONLY_LINES}"
        )[0][0]
        print(f"в staging {total} строк, из них receipt_line {lines}")

        if total == 0:
            print("staging пуст, справочники и факт не трогаем")
            return

        if lines == 0:
            raise RuntimeError("в staging нет строк с event_type='receipt_line'")

        rebuild_dimensions(client)

        client.execute("truncate table piksaev_kiu_db.fact_ecom_orders")
        client.execute(FACT_SQL)

        fact_rows = client.execute("select count() from piksaev_kiu_db.fact_ecom_orders")[0][0]
        print(f"fact_ecom_orders: {fact_rows} строк")
