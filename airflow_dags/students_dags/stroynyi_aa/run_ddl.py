import requests
import warnings

# Отключаем назойливые предупреждения
warnings.filterwarnings("ignore")

CH_HOST = "111.88.243.32"
CH_PORT = 8123
CH_USER = "stroinyi_aa"
CH_PASSWORD = "EWfHrx7wucqd" # <-- ВАШ ПАРОЛЬ

# Список запросов по отдельности
queries = [
    "CREATE DATABASE IF NOT EXISTS stroinyi_aa_db",
    
    """CREATE TABLE IF NOT EXISTS stroinyi_aa_db.fact_orders_raw (
        event_id String, event_time String, receipt_id String, receipt_status String, 
        store_city String, store_region String, product_id String, product_category String, 
        product_brand String, customer_id String, customer_segment String, quantity Int32, 
        gross_amount Float64, discount_amount Float64, net_amount Float64
    ) ENGINE = MergeTree ORDER BY event_id""",
    
    """CREATE TABLE IF NOT EXISTS stroinyi_aa_db.fact_orders (
        order_id String, event_date Date, customer_id String, receipt_status String, 
        store_city String, store_region String, product_id String, product_category String, 
        product_brand String, quantity UInt32, gross_amount Decimal(18, 2), 
        discount_amount Decimal(18, 2), net_amount Decimal(18, 2)
    ) ENGINE = MergeTree ORDER BY (event_date, order_id)""",
    
    """CREATE TABLE IF NOT EXISTS stroinyi_aa_db.dim_customer (
        customer_id String, segment String, customer_type String, country String, city String
    ) ENGINE = ReplacingMergeTree ORDER BY customer_id""",
    
    """CREATE TABLE IF NOT EXISTS stroinyi_aa_db.dim_product (
        product_id String, category String, brand String
    ) ENGINE = ReplacingMergeTree ORDER BY product_id""",
    
    """CREATE TABLE IF NOT EXISTS stroinyi_aa_db.dim_region (
        region_code String, region_name String, country String
    ) ENGINE = ReplacingMergeTree ORDER BY region_code"""
]

url = f"http://{CH_HOST}:{CH_PORT}/"

# Выполняем запросы по очереди
for q in queries:
    response = requests.post(url, params={"query": q}, auth=(CH_USER, CH_PASSWORD))
    if response.status_code != 200:
        print(f"❌ Ошибка при выполнении запроса:\n{q}\n{response.text}")
        break
else:
    print("✅ Все таблицы успешно созданы (или уже существовали)!")

# Проверяем, что получилось
check = requests.post(
    url, 
    params={"query": "SHOW TABLES FROM stroinyi_aa_db"}, 
    auth=(CH_USER, CH_PASSWORD)
)
print("\nСписок таблиц в базе stroinyi_aa_db:")
print(check.text)