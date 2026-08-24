import os
import json
from datetime import datetime
from kafka import KafkaConsumer
import boto3
from botocore.client import Config

# Настройки Kafka - используем явный IP вместо localhost
KAFKA_BOOTSTRAP_SERVERS = ['127.0.0.1:9092']
KAFKA_TOPIC = 'cdc.public.crypto_rates'
GROUP_ID = 's3_loader_group'

# Настройки MinIO (S3)
S3_BUCKET = 'crypto-data-lake'
S3_ENDPOINT = 'http://127.0.0.1:9000'
S3_ACCESS_KEY = 'minioadmin'
S3_SECRET_KEY = 'minioadminpassword'

# Инициализация S3 клиента
s3_client = boto3.client(
    's3',
    endpoint_url=S3_ENDPOINT,
    aws_access_key_id=S3_ACCESS_KEY,
    aws_secret_access_key=S3_SECRET_KEY,
    config=Config(signature_version='s3v4'),
    region_name='us-east-1'
)

def save_to_s3(message_value):
    try:
        data = json.loads(message_value)
        now = datetime.now()
        date_str = now.strftime("%Y-%m-%d")
        timestamp_str = now.strftime("%H%M%S_%f")
        
        # Структурированное хранение по датам
        s3_key = f"raw/crypto_rates/dt={date_str}/{timestamp_str}.json"
        
        s3_client.put_object(
            Bucket=S3_BUCKET,
            Key=s3_key,
            Body=json.dumps(data, ensure_ascii=False, indent=2),
            ContentType='application/json'
        )
        print(f"[{now}] Файл успешно сохранен в S3: {s3_key}")
    except Exception as e:
        print(f"Ошибка при сохранении в S3: {e}")

def main():
    print("Подключение к Kafka брокеру (127.0.0.1:9092)...")
    
    # Создаем консьюмер с жестким указанием версии API Kafka
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=GROUP_ID,
        auto_offset_reset='earliest',  # Читать топик с самого начала истории
        enable_auto_commit=True,
        api_version=(7, 5, 0),         # Соответствует версии образа cp-kafka:7.5.0
        value_deserializer=lambda x: x.decode('utf-8')
    )
    print("Kafka-to-S3 Consumer успешно запущен и слушает топик...")
    
    try:
        for msg in consumer:
            save_to_s3(msg.value)
    except KeyboardInterrupt:
        print("Остановка консьюмера пользователем...")
    finally:
        consumer.close()

if __name__ == "__main__":
    main()
