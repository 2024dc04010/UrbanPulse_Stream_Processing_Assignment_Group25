#!/bin/bash
# ============================================================
# Task C — Submit both Spark Structured Streaming jobs
#
# Prerequisites:
#   1. docker compose up (Spark service healthy)
#   2. Task B Kafka running at localhost:9092
#   3. Smart meter producer running (taskC/smart_meter_producer.py)
#   4. Task B air_quality producer running (for health advisories)
#
# Usage:
#   chmod +x run_spark_jobs.sh
#   ./run_spark_jobs.sh
# ============================================================

set -e

SPARK_CONTAINER="urbanpulse-spark"
KAFKA_PACKAGE="org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0"

echo "============================================================"
echo " UrbanPulse Task C — Spark Job Submission"
echo "============================================================"

# Wait for Spark to be ready
echo "Waiting for Spark master to be ready..."
until docker exec $SPARK_CONTAINER curl -s http://localhost:8080 > /dev/null 2>&1; do
    sleep 2
    echo "  ... still waiting"
done
echo "Spark master is ready."
echo ""

# ── C2: Ward Energy Streaming ─────────────────────────────────────────────────
echo "[1/2] Submitting Ward Energy Streaming job..."
docker exec -d $SPARK_CONTAINER spark-submit \
    --master "local[2]" \
    --packages "$KAFKA_PACKAGE" \
    --conf "spark.driver.memory=1g" \
    --conf "spark.executor.memory=1g" \
    /opt/spark/jobs/ward_energy_streaming.py
echo "      → Submitted: Ward Energy Streaming"
sleep 3

# ── C3: Health Advisories ─────────────────────────────────────────────────────
echo "[2/2] Submitting Health Advisories job..."
docker exec -d $SPARK_CONTAINER spark-submit \
    --master "local[2]" \
    --packages "$KAFKA_PACKAGE" \
    --conf "spark.driver.memory=1g" \
    --conf "spark.executor.memory=1g" \
    /opt/spark/jobs/health_advisories.py
echo "      → Submitted: Health Advisories"

echo ""
echo "Both Spark jobs submitted."
echo "View running jobs at: http://localhost:8080"
echo ""
echo "Output Kafka topics:"
echo "  urbanpulse.ward_energy_summary   (ward energy aggregations)"
echo "  urbanpulse.health_advisories     (AQI advisories for zones > 150 AQI)"
echo ""
echo "Parquet output:"
echo "  ./output/ward_energy/            (partitioned by ward_id and date)"
