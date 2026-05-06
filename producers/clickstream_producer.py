"""
Kafka producer for clickstream events.
Consumes events from the clickstream simulator and sends them to Kafka.
"""
from kafka import KafkaProducer
import json
import time
from clickstream_simulator import ClickstreamSimulator


def create_producer(bootstrap_servers: str = 'localhost:29092') -> KafkaProducer:
    """
    Create and configure a Kafka producer.
    
    Args:
        bootstrap_servers: Kafka bootstrap servers (default: localhost:9092)
        
    Returns:
        Configured KafkaProducer instance
    """
    return KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )


def produce_events(
    topic: str = "clickstream-events",
    bootstrap_servers: str = 'localhost:9092',
    interval: float = 0.5,
    simulator: ClickstreamSimulator = None
) -> None:
    """
    Continuously produce clickstream events to Kafka.
    
    Args:
        topic: Kafka topic to send events to
        bootstrap_servers: Kafka bootstrap servers
        interval: Time interval between events (seconds)
        simulator: ClickstreamSimulator instance (creates default if not provided)
    """
    producer = create_producer(bootstrap_servers)
    simulator = simulator or ClickstreamSimulator()
    
    try:
        while True:
            event = simulator.generate_event()
            producer.send(topic, event)
            print(f"Sent event: {event}")
            time.sleep(interval)
    except KeyboardInterrupt:
        print("\nProducer stopped.")
    finally:
        producer.close()


if __name__ == "__main__":
    produce_events()