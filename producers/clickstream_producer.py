from kafka import KafkaProducer
import json
import random
import time
from datetime import datetime

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

products = ["P100", "P101", "P102", "P103", "P104"]
events = ["view", "add_to_cart", "purchase"]

while True:
    event = {
        "user_id": random.randint(1000, 1100),
        "product_id": random.choice(products),
        "event_type": random.choices(
            events,
            weights=[70, 20, 10]
        )[0],
        "timestamp": datetime.utcnow().isoformat()
    }

    producer.send("clickstream-events", event)
    print(event)
    time.sleep(0.5)