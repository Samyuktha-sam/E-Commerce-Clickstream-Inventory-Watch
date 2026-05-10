import subprocess
import time
import sys
import os
import signal
from pathlib import Path

# Get the project root directory
PROJECT_ROOT = Path(__file__).parent


def run_command(cmd, shell=False, check=True, description=""):
    """Run a shell command and handle errors."""
    if description:
        print(f"\n{'='*70}")
        print(f"▶ {description}")
        print(f"{'='*70}")
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
    except subprocess.CalledProcessError as e:
        print(f"❌ Command failed with exit code {e.returncode}")
        return False
    except Exception as e:
        print(f"❌ Error running command: {e}")
        return False


def wait_for_service(service_name, max_retries=30, delay=2):
    """Wait for a Docker service to be ready."""
    print(f"\n⏳ Waiting for {service_name} to be ready...")

    for attempt in range(max_retries):
        try:
            cmd = f"docker compose ps {service_name}"
            result = subprocess.run(
                cmd, shell=True, cwd=PROJECT_ROOT, capture_output=True, text=True
            )
            # print(f'result.stdout: "{result.stdout.strip()}"')  # Debug output

            # Check if service is running (status contains "running")
            if "Up" in result.stdout:
                print(f"✓ {service_name} is ready")
                return True

        except Exception:
            pass

        if attempt < max_retries - 1:
            time.sleep(delay)

    print(f"⚠ {service_name} health check timeout (continuing anyway...)")
    return False


def main():
    print("\n" + "=" * 70)
    print("🚀 E-Commerce Clickstream Inventory Watch - Full Pipeline")
    print("=" * 70)

    try:
        # Step 1: Start Docker Compose
        if not run_command(
            "docker compose up -d",
            shell=True,
            description="Step 1: Starting Docker Compose (Kafka, Spark, MinIO, Zookeeper)",
        ):
            print("❌ Failed to start Docker Compose")
            sys.exit(1)

        # Wait for key services
        print("\n⏳ Waiting for services to initialize...")
        time.sleep(5)  # Initial startup buffer

        wait_for_service("broker", max_retries=20)
        wait_for_service("spark-master", max_retries=20)
        wait_for_service("minio", max_retries=20)

        print("\n✓ All services initialized")

        # Step 2: Run Clickstream Producer
        print("\n" + "=" * 70)
        print("Step 2: Starting Clickstream Producer (generating events)")
        print("=" * 70)
        print("The producer will run continuously in the background...")

        producer_process = subprocess.Popen(
            f"uv run producers/clickstream_producer.py",
            shell=True,
            cwd=PROJECT_ROOT,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        print(f"✓ Producer started (PID: {producer_process.pid})")

        # Give producer time to start generating events
        print("\n⏳ Letting producer generate events...")
        time.sleep(10)

        # Step 3: Run Spark Stream Processor
        print("\n" + "=" * 70)
        print("Step 3: Starting Spark Stream Processor")
        print("=" * 70)

        spark_cmd = (
            "docker compose exec spark-master /opt/spark/bin/spark-submit "
            "--master spark://spark-master:7077 "
            "--packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1 "
            # "org.apache.hadoop:hadoop-aws:3.4.2 "
            "/opt/spark/work-dir/spark_stream_processor.py"
        )

        success = run_command(
            spark_cmd,
            shell=True,
            description="Running Spark Stream Processor (processes Kafka events)",
        )

        if not success:
            print("\n⚠ Spark job completed or encountered an error")

        # Cleanup
        print("\n" + "=" * 70)
        print("Shutting down...")
        print("=" * 70)

        # Terminate producer
        try:
            producer_process.terminate()
            producer_process.wait(timeout=5)
            print("✓ Producer terminated")
        except subprocess.TimeoutExpired:
            producer_process.kill()
            print("✓ Producer force-killed")
        except Exception as e:
            print(f"⚠ Error terminating producer: {e}")

        # Stop Docker Compose
        print("Stopping Docker Compose services...")
        run_command("docker compose down", shell=True, check=False)

        print("\n✓ All services stopped")
        print("\n" + "=" * 70)
        print("✓ Pipeline completed successfully!")
        print("=" * 70)

    except KeyboardInterrupt:
        print("\n\n⚠ Pipeline interrupted by user")

        # Cleanup on interrupt
        try:
            producer_process.terminate()
        except:
            pass

        subprocess.run("docker compose down", shell=True, cwd=PROJECT_ROOT)
        sys.exit(130)

    except Exception as e:
        print(f"\n❌ Unexpected error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
