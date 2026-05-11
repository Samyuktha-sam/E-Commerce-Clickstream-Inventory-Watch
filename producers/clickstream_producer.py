"""Kafka producer for clickstream events."""

from __future__ import annotations

import json
import logging
import os
import time

from kafka import KafkaProducer

from clickstream_simulator import ClickstreamSimulator

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


def create_producer(bootstrap_servers: str = "localhost:9092") -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
    )


def produce_events(
    topic: str = "clickstream-events",
    bootstrap_servers: str | None = None,
    interval: float = 0.5,
    simulator: ClickstreamSimulator | None = None,
) -> None:
    bootstrap_servers = bootstrap_servers or os.getenv(
        "KAFKA_PRODUCER_BOOTSTRAP_SERVERS", "localhost:9092"
    )
    producer = create_producer(bootstrap_servers)
    simulator = simulator or ClickstreamSimulator()

    logger.info("Producing clickstream events to %s via %s", topic, bootstrap_servers)
    try:
        while True:
            event = simulator.generate_event()
            producer.send(topic, event).get(timeout=30)
            logger.info(
                "Sent %-11s event for product=%s user=%s",
                event["event_type"],
                event["product_id"],
                event["user_id"],
            )
            time.sleep(interval)
    except KeyboardInterrupt:
        logger.info("Producer stopped by user.")
    finally:
        producer.flush()
        producer.close()


if __name__ == "__main__":
    produce_events(interval=float(os.getenv("PRODUCER_INTERVAL_SECONDS", "0.5")))
