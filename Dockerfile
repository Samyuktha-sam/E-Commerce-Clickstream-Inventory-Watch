FROM apache/spark:4.1.1-scala2.13-java17-python3-ubuntu

USER root

# Install system dependencies
RUN apt-get update && apt-get install -y \
    curl wget netcat-traditional \
    python3-pip \
    && rm -rf /var/lib/apt/lists/*

# Install Airflow + Spark Provider + common packages
RUN pip3 install --no-cache-dir --upgrade pip && \
    pip3 install --no-cache-dir \
    apache-airflow==2.10.0 \
    apache-airflow-providers-apache-spark \
    apache-airflow-providers-amazon \
    kafka-python \
    minio \
    delta-spark \
    pandas numpy matplotlib seaborn pyarrow \
    jupyterlab notebook

# Create directories
RUN mkdir -p /opt/spark/jars/extra /opt/airflow/dags /opt/airflow/logs /opt/airflow/plugins

# Download JARs for MinIO + Kafka
WORKDIR /opt/spark/jars/extra

RUN wget -q https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.13/4.1.1/spark-sql-kafka-0-10_2.13-4.1.1.jar && \
    wget -q https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.4.0/hadoop-aws-3.4.0.jar && \
    wget -q https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.777/aws-java-sdk-bundle-1.12.777.jar

# Set environment variables
ENV AIRFLOW_HOME=/opt/airflow \
    SPARK_HOME=/opt/spark \
    PYTHONPATH=/opt/airflow \
    AIRFLOW__CORE__LOAD_EXAMPLES=false \
    AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=sqlite:////opt/airflow/airflow.db

# Initialize Airflow DB and create user
RUN airflow db init && \
    airflow users create \
        --username admin \
        --firstname Admin \
        --lastname User \
        --role Admin \
        --email admin@example.com \
        --password admin || true

WORKDIR /opt/airflow

EXPOSE 8888 8080 4040 9000 9001

# Default command: Start Jupyter (you can override in compose)
CMD ["jupyter", "lab", "--ip=0.0.0.0", "--port=8888", "--no-browser", "--allow-root"]
