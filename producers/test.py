# from clickstream_simulator import ClickstreamSimulator

# while True:
#     simulator = ClickstreamSimulator()
#     event = simulator.generate_event()
#     print(event)


from kafka import KafkaConsumer
consumer = KafkaConsumer(
    'alerts-notifications',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='clickstream-consumers',
    value_deserializer=lambda x: x.decode('utf-8')
)
print("Consuming events from Kafka topic 'clickstream-events'...")
for message in consumer:
    print(f"Received event: {message.value}")
