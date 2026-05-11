# E-Commerce Clickstream Inventory Watch

Minimal MVP lambda-architecture pipeline for the assessment scenario using:

- Kafka 4.1.1 in KRaft mode
- Spark 4.1.1 for streaming and batch
- Airflow 3.2.1 for orchestration
- Delta Lake 4.0.0 on MinIO
- PostgreSQL 16 for Airflow metadata
- Mailpit for local email delivery

## What the pipeline does

- `producers/clickstream_producer.py` generates synthetic clickstream events.
- `stream_layer/spark_stream_processor.py` reads Kafka, detects high-interest/low-conversion products, and publishes alerts to `alerts-notifications`.
- `stream_layer/spark_raw_sink.py` persists raw clickstream events to Delta Lake in MinIO.
- `airflow/dags/daily_clickstream_batch_dag.py` orchestrates the daily batch job.
- `batch/spark_daily_batch_job.py` creates:
  - daily user segmentation
  - top 5 most viewed products
  - conversion rates by product category
  - a local email-ready text summary
  - a JSON payload on the `batch-results` Kafka topic

## Run the stack

```bash
docker compose up -d --build
```

Services:

- Kafka: `localhost:9092`
- Spark master UI: `http://localhost:8085`
- Airflow UI/API: `http://localhost:8087` (`admin` / `admin`)
- MinIO console: `http://localhost:9001` (`minioadmin` / `minioadmin`)
- Mailpit UI: `http://localhost:8025`

## Start the streaming layer

Open separate terminals:

```bash
python /home/runner/work/E-Commerce-Clickstream-Inventory-Watch/E-Commerce-Clickstream-Inventory-Watch/producers/clickstream_producer.py
```

```bash
docker compose exec -T spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1,io.delta:delta-spark_2.13:4.0.0,org.apache.hadoop:hadoop-aws:3.4.2 \
  /opt/pipeline/stream_layer/spark_stream_processor.py
```

```bash
docker compose exec -T spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1,io.delta:delta-spark_2.13:4.0.0,org.apache.hadoop:hadoop-aws:3.4.2 \
  /opt/pipeline/stream_layer/spark_raw_sink.py
```

Or run the helper launcher:

```bash
python /home/runner/work/E-Commerce-Clickstream-Inventory-Watch/E-Commerce-Clickstream-Inventory-Watch/main.py
```

## Trigger the batch job

```bash
docker compose exec -T airflow-api-server airflow dags trigger daily_clickstream_batch_processing
```

Outputs:

- Delta raw lake: `s3a://clickstream-lake/raw-events`
- Daily report JSON: `s3a://clickstream-lake/reports/report_date=<YYYY-MM-DD>/...`
- Email-ready text summary: `/opt/airflow/reports/daily_summary.txt` inside the Airflow containers
- Email preview: Mailpit UI
- Kafka batch topic: `batch-results`

## Notes

- Flash-sale thresholds default to the assessment values: more than `100` views and fewer than `5` purchases in a 10-minute sliding window.
- For a quick local demo, lower `FLASH_SALE_VIEW_THRESHOLD` in `.env` before starting the stack.
- All critical service versions are pinned to avoid the previous image/API mismatch issues.
