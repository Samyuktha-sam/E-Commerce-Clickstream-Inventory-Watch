# Variables to make it easy to update later
MASTER_CONTAINER = spark-master
SPARK_MASTER_URL = spark://spark-master:7077
TEST_FILE = /opt/spark/work-dir/test.py
KAFKA_PKG = org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1

.PHONY: run-test consumer

# Run the Kafka Test Writer with optimized settings
run-test:
	docker compose exec $(MASTER_CONTAINER) /opt/spark/bin/spark-submit \
		--master $(SPARK_MASTER_URL) \
		--conf spark.cores.max=2 \
		--conf spark.executor.cores=2 \
		--conf spark.driver.memory=1g \
		--conf spark.executor.memory=1g \
		--packages $(KAFKA_PKG) \
		$(TEST_FILE)

# Helper to watch the Kafka topic
watch-kafka:
	docker compose exec broker kafka-console-consumer \
		--bootstrap-server localhost:9092 \
		--topic alerts-notifications \
		--from-beginning

run-raw-sink:
	docker compose exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1,org.apache.hadoop:hadoop-aws:3.4.2 --conf spark.sql.streaming.metricsEnabled=false  --conf spark.sql.streaming.ui.enabled=false   --conf spark.ui.showConsoleProgress=false --conf spark.sql.streaming.progressReportInterval=0 --conf spark.cores.max=2 --conf spark.executor.cores=2 --conf spark.driver.memory=1g --conf spark.executor.memory=1g   /opt/spark/work-dir/spark_raw_sink.py  