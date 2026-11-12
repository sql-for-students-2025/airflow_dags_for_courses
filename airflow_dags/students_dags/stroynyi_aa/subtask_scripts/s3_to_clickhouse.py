import os
import boto3
import urllib3
from airflow.models import Variable
from clickhouse_driver import Client

urllib3.disable_warnings()

S3_BUCKET = "dad-ksu-datalab"
S3_ENDPOINT = "https://storage.yandexcloud.net"
MY_LOGIN = "stroinyi_aa"  
CH_HOST = "111.88.243.32" 

def upload_to_s3(**context) -> None:
    access_key = Variable.get("s3_dad_ksu_dataplatform_access_key")
    secret_key = Variable.get("s3_dad_ksu_dataplatform_secret_key")

    # Сами восстанавливаем путь к файлу по run_id (без XCom)
    run_id = context["run_id"].replace(":", "_").replace("+", "_")
    local_file = f"/tmp/rabbit_{run_id}/{run_id}.json"
    
    # Если папка называется иначе (mkdtemp создает рандомный суффикс), найдем файл
    import glob
    matching_files = glob.glob(f"/tmp/rabbit_*/{run_id}.json")
    if matching_files:
        local_file = matching_files[0]
    else:
        raise FileNotFoundError(f"Не найден файл для run_id {run_id}")

    s3_key = f"DATA/{MY_LOGIN}/{run_id}.json"

    s3 = boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        verify=False
    )
    s3.upload_file(local_file, S3_BUCKET, s3_key)


def load_s3_to_clickhouse(**context) -> None:
    access_key = Variable.get("s3_dad_ksu_dataplatform_access_key")
    secret_key = Variable.get("s3_dad_ksu_dataplatform_secret_key")
    ch_user = Variable.get("clickhouse_tech_user_account")
    ch_pass = Variable.get("clickhouse_tech_user_password")

    # Восстанавливаем имя ключа S3 по run_id (без XCom)
    run_id = context["run_id"].replace(":", "_").replace("+", "_")
    s3_key = f"DATA/{MY_LOGIN}/{run_id}.json"

    s3 = boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        verify=False
    )
    tmp_path = f"/tmp/{run_id}_download.json"
    s3.download_file(S3_BUCKET, s3_key, tmp_path)

    with open(tmp_path, "r", encoding="utf-8") as f:
        raw_text = f.read()

    with Client(
        host=CH_HOST,
        user=ch_user,
        password=ch_pass,
        database=f"{MY_LOGIN}_db",
        port=9000,
    ) as client:
        # 1. Загружаем в сырую таблицу
        insert_query = f"INSERT INTO fact_orders_raw FORMAT JSONEachRow\n{raw_text}"
        client.execute(insert_query)

        # 2. Заполняем справочники из сырой таблицы (Исправлено!)
        
        # 2.1. dim_customer
        client.execute("""
            INSERT INTO dim_customer (customer_id, segment, customer_type, country, city)
            SELECT DISTINCT 
                customer_id, 
                customer_segment AS segment, 
                'Физлицо' AS customer_type, 
                'Россия' AS country, 
                'Город' AS city
            FROM fact_orders_raw
            WHERE customer_id NOT IN (SELECT customer_id FROM dim_customer)
        """)

        # 2.2. dim_product
        client.execute("""
            INSERT INTO dim_product (product_id, category, brand)
            SELECT DISTINCT 
                product_id, 
                product_category AS category, 
                product_brand AS brand
            FROM fact_orders_raw
            WHERE product_id NOT IN (SELECT product_id FROM dim_product)
        """)

        # 2.3. dim_region
        client.execute("""
            INSERT INTO dim_region (region_code, region_name, country)
            SELECT DISTINCT 
                concat(store_region, '_', store_city) AS region_code, 
                store_city AS region_name, 
                'Россия' AS country
            FROM fact_orders_raw
            WHERE concat(store_region, '_', store_city) NOT IN (SELECT region_code FROM dim_region)
        """)

        # 3. Заполняем таблицу фактов
        client.execute("""
            INSERT INTO fact_orders
            SELECT
                event_id                                    AS order_id,
                toDate(parseDateTimeBestEffort(event_time)) AS event_date,
                customer_id,
                receipt_status,
                store_city,
                store_region,
                product_id,
                product_category,
                product_brand,
                toUInt32(quantity)                          AS quantity,
                toDecimal64(gross_amount, 2)                AS gross_amount,
                toDecimal64(discount_amount, 2)             AS discount_amount,
                toDecimal64(net_amount, 2)                  AS net_amount
            FROM fact_orders_raw
            WHERE event_id NOT IN (SELECT order_id FROM fact_orders)
        """)