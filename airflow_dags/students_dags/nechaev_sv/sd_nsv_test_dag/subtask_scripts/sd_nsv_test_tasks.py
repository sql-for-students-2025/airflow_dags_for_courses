import importlib

VARIABLES = [
    "s3_dad_ksu_dataplatform_access_key",
    "s3_dad_ksu_dataplatform_secret_key",
    "rabbit_mq_svc_login",
    "rabbit_mq_svc_password",
    "rabbit_mq_host",
    "clickhouse_tech_user_account",
    "clickhouse_tech_user_password",
    "clickhouse_host",
]

LIBRARIES = ["pika", "boto3", "clickhouse_driver"]


def hello(name="User"):
    print(f"SUCCESS {name}!!!")


def check_environment():
    from airflow.models import Variable

    missing = [name for name in VARIABLES if Variable.get(name, default_var=None) is None]
    for name in VARIABLES:
        print(f"Variable {name}: {'нет' if name in missing else 'есть'}")

    for library in LIBRARIES:
        module = importlib.import_module(library)
        print(f"Библиотека {library}: {getattr(module, '__version__', 'ok')}")

    if missing:
        raise RuntimeError(f"Не заданы переменные Airflow: {missing}")
