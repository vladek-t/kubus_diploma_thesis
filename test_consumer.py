import logging
logging.basicConfig(level=logging.DEBUG)

from kafka import KafkaConsumer, TopicPartition

consumer = KafkaConsumer(
    bootstrap_servers='127.0.0.1:9092',
    auto_offset_reset='earliest',
    enable_auto_commit=False,
    consumer_timeout_ms=10000
)
tp = TopicPartition('cdc.public.crypto_rates', 0)
consumer.assign([tp])
consumer.seek_to_beginning()
print("Assignment:", consumer.assignment())
print("Position:", consumer.position(tp))
records = consumer.poll(timeout_ms=5000)
print("Polled records count:", len(records))
for tp, msgs in records.items():
    for msg in msgs:
        print(msg.value[:200])
consumer.close()