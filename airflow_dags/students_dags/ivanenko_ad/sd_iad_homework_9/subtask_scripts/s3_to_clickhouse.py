from airflow.models import Variable
from clickhouse_driver import Client

def load_s3_to_clickhouse(s3_path, **kwargs):
    file_url = f"dad-ksu-datalab/{s3_path}"

    ch_client = Client(
        host=Variable.get("clickhouse_host"),
        port=int(Variable.get("clickhouse_port", 9000)), 
        user=Variable.get("clickhouse_tech_user_account"),
        password=Variable.get("clickhouse_tech_user_password"),
        database="ivanenko_ad_db"
    )
    

    query = f"""
        INSERT INTO raw_sales
        SELECT * FROM s3(
            s3_dad_ksu_datalab, 
            filename='{file_url}',
            format='JSONEachRow'
        )
    """
    ch_client.execute(query)
