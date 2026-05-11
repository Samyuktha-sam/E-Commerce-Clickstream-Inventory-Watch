from __future__ import annotations

import subprocess
import sys
import time
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent
STREAM_PACKAGES = (
    "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1,"
    "io.delta:delta-spark_2.13:4.0.0,"
    "org.apache.hadoop:hadoop-aws:3.4.2"
)


def run_command(cmd, shell=False, check=True, description=""):
    if description:
        print(f"\n{'=' * 70}")
        print(f"▶ {description}")
        print(f"{'=' * 70}")
        print(f"Command: {cmd if isinstance(cmd, str) else ' '.join(cmd)}")

    try:
        result = subprocess.run(
            cmd,
            shell=shell,
            cwd=PROJECT_ROOT,
            check=check,
            capture_output=False,
            text=True,
        )
        return result.returncode == 0
    except subprocess.CalledProcessError as exc:
        print(f"❌ Command failed with exit code {exc.returncode}")
        return False
    except Exception as exc:
        print(f"❌ Error running command: {exc}")
        return False


def wait_for_service(service_name, max_retries=30, delay=2):
    print(f"\n⏳ Waiting for {service_name} to be ready...")
    for attempt in range(max_retries):
        result = subprocess.run(
            f"docker compose ps {service_name}",
            shell=True,
            cwd=PROJECT_ROOT,
            capture_output=True,
            text=True,
        )
        if "Up" in result.stdout or "healthy" in result.stdout:
            print(f"✓ {service_name} is ready")
            return True
        if attempt < max_retries - 1:
            time.sleep(delay)
    print(f"⚠ {service_name} health check timeout (continuing anyway...)")
    return False


def start_background_process(description: str, command: str):
    print(f"\n{'=' * 70}\n{description}\n{'=' * 70}")
    process = subprocess.Popen(
        command,
        shell=True,
        cwd=PROJECT_ROOT,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    print(f"✓ Started (PID: {process.pid})")
    return process


def terminate_process(process, name: str):
    if not process:
        return
    try:
        process.terminate()
        process.wait(timeout=5)
        print(f"✓ {name} terminated")
    except subprocess.TimeoutExpired:
        process.kill()
        print(f"✓ {name} force-killed")


def main():
    print("\n" + "=" * 70)
    print("🚀 E-Commerce Clickstream Inventory Watch - MVP Lambda Pipeline")
    print("=" * 70)

    producer_process = None
    stream_process = None
    sink_process = None

    try:
        if not run_command(
            "docker compose up -d --build",
            shell=True,
            description="Step 1: Starting Docker Compose stack",
        ):
            sys.exit(1)

        time.sleep(5)
        for service in ("broker", "spark-master", "minio", "airflow-api-server"):
            wait_for_service(service, max_retries=30)

        stream_process = start_background_process(
            "Step 2: Starting Spark stream processor",
            "docker compose exec -T spark-master /opt/spark/bin/spark-submit "
            "--master spark://spark-master:7077 "
            f"--packages {STREAM_PACKAGES} "
            "/opt/pipeline/stream_layer/spark_stream_processor.py",
        )
        sink_process = start_background_process(
            "Step 3: Starting Spark raw sink",
            "docker compose exec -T spark-master /opt/spark/bin/spark-submit "
            "--master spark://spark-master:7077 "
            f"--packages {STREAM_PACKAGES} "
            "/opt/pipeline/stream_layer/spark_raw_sink.py",
        )

        time.sleep(10)
        producer_process = start_background_process(
            "Step 4: Starting clickstream producer",
            "python producers/clickstream_producer.py",
        )

        print("\n⏳ Streaming jobs are running. Trigger the Airflow DAG from http://localhost:8087 when ready.")
        print("📧 Mailpit UI: http://localhost:8025")
        print("Press Ctrl+C to stop the stack.")
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n\n⚠ Pipeline interrupted by user")
    finally:
        terminate_process(producer_process, "Producer")
        terminate_process(stream_process, "Spark Stream Processor")
        terminate_process(sink_process, "Spark Raw Sink")
        run_command("docker compose down", shell=True, check=False, description="Stopping Docker Compose services")


if __name__ == "__main__":
    main()
