from kafka import KafkaConsumer
import json
from streaming.src.processor import process_event

consumer = KafkaConsumer(
    "clickstream-events",
    bootstrap_servers="localhost:9092",
    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
)

print("Listening for events from Kafka topic 'clickstream-events'...")

for message in consumer:
    event = message.value
    print("Event:", event)

    summary, alert = process_event(event)

    print("Summary:", summary)

    if alert:
        print("ALERT:", alert)
