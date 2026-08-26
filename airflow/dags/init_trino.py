from airflow import DAG
from airflow.providers.trino.operators.trino import TrinoOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 1, 1),
    'retries': 1,
}

with DAG(
    dag_id='init_trino_schema',
    default_args=default_args,
    schedule_interval=None,          # только ручной запуск
    catchup=False,
    tags=['init'],
) as dag:

    create_schema = TrinoOperator(
        task_id='create_schema',
        sql="CREATE SCHEMA IF NOT EXISTS minio.crypto WITH (location = 's3://crypto-data-lake/raw/crypto_rates/')",
        trino_conn_id='trino_default',
    )

    create_table = TrinoOperator(
        task_id='create_table',
        sql="""
        CREATE TABLE IF NOT EXISTS minio.crypto.crypto_rates (
            id INTEGER,
            coin_symbol VARCHAR,
            coin_name VARCHAR,
            price_usd DOUBLE,
            volume_24h DOUBLE,
            updated_at VARCHAR,
            _op VARCHAR
        )
        WITH (
            format = 'JSON',
            external_location = 's3://crypto-data-lake/raw/crypto_rates/'
        )
        """,
        trino_conn_id='trino_default',
    )

    create_schema >> create_table