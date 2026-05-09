"""
Serving Layer — Speed bridge: Kafka → ClickHouse
Reads from two Kafka topics in parallel:
  • clickstream-events  → raw_events table
  • alerts-notifications → flash_sale_alerts table

Run continuously alongside the Spark streaming job.
"""

import json
import logging
import os
import threading
from datetime import datetime

from clickhouse_driver import Client
from kafka import KafkaConsumer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(threadName)s] %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration (env-overridable)
# ---------------------------------------------------------------------------
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "localhost")
CLICKHOUSE_PORT = int(os.getenv("CLICKHOUSE_PORT", "9000"))
CLICKHOUSE_DB   = os.getenv("CLICKHOUSE_DB", "clickstream")

EVENTS_TOPIC = os.getenv("KAFKA_TOPIC", "clickstream-events")
ALERTS_TOPIC = os.getenv("ALERTS_TOPIC", "alerts-notifications")

BATCH_SIZE    = int(os.getenv("CH_BATCH_SIZE", "100"))
BATCH_TIMEOUT = float(os.getenv("CH_BATCH_TIMEOUT_SEC", "5.0"))


# ---------------------------------------------------------------------------
# ClickHouse helpers
# ---------------------------------------------------------------------------

def get_ch_client() -> Client:
    """Create a ClickHouse driver client."""
    return Client(
        host=CLICKHOUSE_HOST,
        port=CLICKHOUSE_PORT,
        database=CLICKHOUSE_DB,
    )


def insert_raw_events(client: Client, rows: list[dict]) -> None:
    """Bulk-insert a batch of raw clickstream events."""
    data = []
    for r in rows:
        try:
            data.append({
                "event_id":   str(r.get("event_id", "")),
                "user_id":    str(r.get("user_id", "")),
                "product_id": str(r.get("product_id", "")),
                "event_type": str(r.get("event_type", "")),
                "event_time": datetime.fromisoformat(r["timestamp"])
                              if r.get("timestamp") else datetime.utcnow(),
            })
        except Exception as exc:
            logger.warning("Skipping malformed event: %s — %s", r, exc)

    if data:
        client.execute(
            "INSERT INTO raw_events "
            "(event_id, user_id, product_id, event_type, event_time) VALUES",
            data,
        )
        logger.info("Inserted %d raw events into ClickHouse", len(data))


def insert_alert(client: Client, rows: list[dict]) -> None:
    """Bulk-insert flash-sale alert records."""
    data = []
    for r in rows:
        try:
            data.append({
                "window_start":   datetime.fromisoformat(str(r.get("start", ""))),
                "window_end":     datetime.fromisoformat(str(r.get("end", ""))),
                "product_id":     str(r.get("product_id", "")),
                "view_count":     int(r.get("view_count", 0)),
                "purchase_count": int(r.get("purchase_count", 0)),
                "action":         str(r.get("action", "")),
            })
        except Exception as exc:
            logger.warning("Skipping malformed alert: %s — %s", r, exc)

    if data:
        client.execute(
            "INSERT INTO flash_sale_alerts "
            "(window_start, window_end, product_id, view_count, purchase_count, action) VALUES",
            data,
        )
        logger.info("Inserted %d flash-sale alerts into ClickHouse", len(data))


# ---------------------------------------------------------------------------
# Consumer threads
# ---------------------------------------------------------------------------

def consume_events() -> None:
    """Consume clickstream-events and write to ClickHouse in micro-batches."""
    consumer = KafkaConsumer(
        EVENTS_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        group_id="ch-events-consumer",
        auto_offset_reset="latest",
        enable_auto_commit=True,
    )
    client = get_ch_client()
    logger.info("Events consumer started on topic: %s", EVENTS_TOPIC)

    buffer: list[dict] = []
    last_flush = datetime.utcnow()

    for message in consumer:
        buffer.append(message.value)
        elapsed = (datetime.utcnow() - last_flush).total_seconds()

        if len(buffer) >= BATCH_SIZE or elapsed >= BATCH_TIMEOUT:
            try:
                insert_raw_events(client, buffer)
            except Exception as exc:
                logger.error("ClickHouse insert failed (events): %s", exc)
                client = get_ch_client()  # Reconnect
            buffer = []
            last_flush = datetime.utcnow()


def consume_alerts() -> None:
    """Consume alerts-notifications and write to ClickHouse in micro-batches."""
    consumer = KafkaConsumer(
        ALERTS_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        group_id="ch-alerts-consumer",
        auto_offset_reset="latest",
        enable_auto_commit=True,
    )
    client = get_ch_client()
    logger.info("Alerts consumer started on topic: %s", ALERTS_TOPIC)

    buffer: list[dict] = []
    last_flush = datetime.utcnow()

    for message in consumer:
        buffer.append(message.value)
        elapsed = (datetime.utcnow() - last_flush).total_seconds()

        if len(buffer) >= BATCH_SIZE or elapsed >= BATCH_TIMEOUT:
            try:
                insert_alert(client, buffer)
            except Exception as exc:
                logger.error("ClickHouse insert failed (alerts): %s", exc)
                client = get_ch_client()
            buffer = []
            last_flush = datetime.utcnow()


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    logger.info(
        "Starting Kafka→ClickHouse bridge  kafka=%s  ch=%s:%s/%s",
        KAFKA_BOOTSTRAP, CLICKHOUSE_HOST, CLICKHOUSE_PORT, CLICKHOUSE_DB,
    )

    t_events = threading.Thread(
        target=consume_events, name="events-consumer", daemon=True
    )
    t_alerts = threading.Thread(
        target=consume_alerts, name="alerts-consumer", daemon=True
    )

    t_events.start()
    t_alerts.start()

    # Keep main thread alive; threads are daemon so Ctrl-C exits cleanly
    t_events.join()
    t_alerts.join()


if __name__ == "__main__":
    main()