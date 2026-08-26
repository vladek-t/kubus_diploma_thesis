from airflow import DAG
from airflow.providers.trino.operators.trino import TrinoOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 1, 1),
    'retries': 1,
}

with DAG(
    dag_id='build_crypto_mart',
    default_args=default_args,
    schedule_interval='@hourly',   # или None для ручного запуска
    catchup=False,
    tags=['mart'],
) as dag:

    drop_summary = TrinoOperator(
        task_id='drop_crypto_summary',
        sql="DROP TABLE IF EXISTS minio.crypto.crypto_summary_24h",
        trino_conn_id='trino_default',
    )

    create_summary = TrinoOperator(
        task_id='create_summary',
        sql="""
        CREATE TABLE minio.crypto.crypto_summary_24h
        WITH (
            format = 'PARQUET',
            external_location = 's3://crypto-data-lake/marts/crypto_summary_24h'
        )
        AS
        SELECT
            coin_symbol,
            coin_name,
            COUNT(*) AS data_points,
            AVG(price_usd) AS avg_price,
            MIN(price_usd) AS min_price,
            MAX(price_usd) AS max_price,
            MAX_BY(price_usd, from_iso8601_timestamp(recorded_at)) AS last_price
        FROM minio.crypto.crypto_rates_history
        WHERE from_iso8601_timestamp(recorded_at) >= NOW() - INTERVAL '24' HOUR
        GROUP BY coin_symbol, coin_name
        """,
        trino_conn_id='trino_default',
    )

    drop_summary >> create_summary