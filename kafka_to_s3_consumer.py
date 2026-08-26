import json
from datetime import datetime
from kafka import KafkaConsumer
import boto3
from botocore.client import Config
import time

KAFKA_BOOTSTRAP_SERVERS = ['127.0.0.1:9092']
KAFKA_TOPICS = ['cdc.public.crypto_rates', 'cdc.public.crypto_rates_history']
GROUP_ID = 's3_loader_group'

S3_BUCKET = 'crypto-data-lake'
S3_ENDPOINT = 'http://127.0.0.1:9000'
S3_ACCESS_KEY = 'minioadmin'
S3_SECRET_KEY = 'minioadminpassword'

s3_client = boto3.client(
    's3',
    endpoint_url=S3_ENDPOINT,
    aws_access_key_id=S3_ACCESS_KEY,
    aws_secret_access_key=S3_SECRET_KEY,
    config=Config(signature_version='s3v4'),
    region_name='us-east-1'
)

def ensure_bucket_exists(bucket_name):
    try:
        s3_client.head_bucket(Bucket=bucket_name)
        print(f"Бакет {bucket_name} уже существует")
    except:
        s3_client.create_bucket(Bucket=bucket_name)
        print(f"Создан бакет {bucket_name}")

def save_to_s3(message_value, topic):
    try:
        data = json.loads(message_value)
        payload = data.get('payload')
        if not payload:
            return
        op = payload.get('op')
        after = payload.get('after')
        before = payload.get('before')

        if op in ('c', 'u', 'r') and after is not None:
            row = after
        elif op == 'd' and before is not None:
            row = before
        else:
            return

        row['_op'] = op

        now = datetime.now()
        timestamp_str = now.strftime("%H%M%S_%f")

        if 'history' in topic:
            folder = 'raw/crypto_rates_history'
        else:
            folder = 'raw/crypto_rates'

        s3_key = f"{folder}/{timestamp_str}.json"

        s3_client.put_object(
            Bucket=S3_BUCKET,
            Key=s3_key,
            Body=json.dumps(row, ensure_ascii=False),
            ContentType='application/json'
        )
        print(f"[{now}] Файл сохранён: {s3_key}")
    except Exception as e:
        print(f"Ошибка при сохранении: {e}")

def main():
    print("Подключение к Kafka брокеру...")
    ensure_bucket_exists(S3_BUCKET)

    consumer = KafkaConsumer(
        *KAFKA_TOPICS,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=GROUP_ID,
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        value_deserializer=lambda x: x.decode('utf-8')
    )
    print("Kafka-to-S3 Consumer запущен и слушает топики...")

    try:
        for msg in consumer:
            save_to_s3(msg.value, msg.topic)
    except KeyboardInterrupt:
        print("Остановка консьюмера пользователем...")
    finally:
        consumer.close()

if __name__ == "__main__":
    main()