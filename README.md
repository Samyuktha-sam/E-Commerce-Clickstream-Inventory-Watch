run flash sale detector with:
```bash
pwsh -Command "docker compose exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 --conf spark.cores.max=2 --conf spark.executor.cores=2 --conf spark.driver.memory=1g --conf spark.executor.memory=2g --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1 /opt/spark/work-dir/spark_stream_processor.py"
```

run archive metastore with:
```bash
 docker compose exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1,org.apache.hadoop:hadoop-aws:3.4.2 --conf spark.sql.streaming.metricsEnabled=false  --conf spark.sql.streaming.ui.enabled=false   --conf spark.ui.showConsoleProgress=false --conf spark.sql.streaming.progressReportInterval=0 --conf spark.cores.max=2 --conf spark.executor.cores=2 --conf spark.driver.memory=1g --conf spark.executor.memory=1g   /opt/spark/work-dir/spark_raw_sink.py
```