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
    schedule_interval='@hourly',   # можно настроить по расписанию
    catchup=False,
    tags=['mart'],
) as dag:

    create_mart_table = TrinoOperator(
        task_id='create_crypto_summary',
        sql="""
        CREATE TABLE IF NOT EXISTS minio.crypto.crypto_summary_24h AS
        SELECT
            coin_symbol,
            coin_name,
            COUNT(*) AS data_points,
            AVG(price_usd) AS avg_price,
            MIN(price_usd) AS min_price,
            MAX(price_usd) AS max_price,
            MAX_BY(price_usd, recorded_at) AS last_price
        FROM minio.crypto.crypto_rates_history
        WHERE recorded_at >= NOW() - INTERVAL '24' HOUR
        GROUP BY coin_symbol, coin_name
        """,
        trino_conn_id='trino_default',
    )

    # Опционально можно вставить результаты в другую таблицу или просто оставить как есть