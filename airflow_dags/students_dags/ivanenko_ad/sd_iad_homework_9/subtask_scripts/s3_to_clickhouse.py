import clickhouse_connect
from airflow.models import Variable

def load_s3_to_clickhouse(s3_path, **kwargs):
    endpoint_url = 'https://storage.yandexcloud.net'
    access = Variable.get("s3_dad_ksu_dataplatform_access_key")
    secret = Variable.get("s3_dad_ksu_dataplatform_secret_key")
    bucket = 'dad-ksu-datalab'
    
    file_url = f"{endpoint_url}/{bucket}/{s3_path}"

    
    ch_client = clickhouse_connect.get_client(
        host=Variable.get("clickhouse_host"), 
        port=int(Variable.get("clickhouse_port")),
        user=Variable.get("clickhouse_tech_user_account"),
        password=Variable.get("clickhouse_tech_user_password"),
        database="ivanenko_ad_db"
    )
    
    query = f"""
        INSERT INTO raw_sales
        SELECT * FROM s3('{file_url}', '{access}', '{secret}', 'JSONEachRow')
    """
    ch_client.command(query)