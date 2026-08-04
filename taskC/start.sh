#!/bin/bash
# ============================================================
# Task C — Full Demo Orchestration Script
#
# Starts the entire Task C pipeline end-to-end:
#   1. Docker services (Flink + Spark)
#   2. Kafka topic creation
#   3. Smart meter producer (background)
#   4. Flink jobs (background, inside containers)
#   5. Spark jobs (background, inside containers)
#
# Prerequisites:
#   - Docker and Docker Compose installed
#   - Task B Kafka running at localhost:9092
#   - Task B producers running (air_quality, traffic_signals, bus_gps)
#   - pip install kafka-python (for smart_meter_producer.py)
#
# Usage:
#   cd taskC/
#   chmod +x start.sh
#   ./start.sh
#
# To stop everything:
#   ./stop.sh
# ============================================================

set -e
cd "$(dirname "$0")"   # Always run from taskC/ directory

echo ""
echo "╔══════════════════════════════════════════════════════════╗"
echo "║  UrbanPulse Task C — Full Pipeline Startup               ║"
echo "╚══════════════════════════════════════════════════════════╝"
echo ""

# ── Step 1: Start Docker services ────────────────────────────────────────────
echo "▶ Step 1: Starting Docker services (Flink + Spark)..."
docker compose up -d --build
echo "  Docker services started. Waiting for healthy state..."
sleep 10

# ── Step 2: Create Kafka topics ───────────────────────────────────────────────
echo ""
echo "▶ Step 2: Creating Task C Kafka topics..."
# Try to create topics - requires kafka-topics.sh on PATH
# If not available locally, use the Flink container
if command -v kafka-topics.sh &>/dev/null; then
    bash kafka/create_taskc_topics.sh
else
    echo "  kafka-topics.sh not on PATH — creating topics via Docker..."
    # Fall back to using the flink container (which has Kafka client tools if available)
    docker run --rm --network host confluentinc/cp-kafka:7.5.0 \
        kafka-topics --create --if-not-exists --bootstrap-server localhost:9092 \
        --topic urbanpulse.incidents --partitions 3 --replication-factor 1
    docker run --rm --network host confluentinc/cp-kafka:7.5.0 \
        kafka-topics --create --if-not-exists --bootstrap-server localhost:9092 \
        --topic urbanpulse.ward_energy_summary --partitions 3 --replication-factor 1
    docker run --rm --network host confluentinc/cp-kafka:7.5.0 \
        kafka-topics --create --if-not-exists --bootstrap-server localhost:9092 \
        --topic urbanpulse.health_advisories --partitions 3 --replication-factor 1
fi
echo "  Kafka topics ready."

# ── Step 3: Start Smart Meter Producer ───────────────────────────────────────
echo ""
echo "▶ Step 3: Starting Smart Meter Producer (background)..."
python3 smart_meter_producer.py > logs/smart_meter_producer.log 2>&1 &
PRODUCER_PID=$!
echo "  Smart Meter Producer started (PID: $PRODUCER_PID)"

# ── Step 4: Submit Flink Jobs ─────────────────────────────────────────────────
echo ""
echo "▶ Step 4: Submitting Flink incident detection jobs..."
bash run_flink_jobs.sh

# ── Step 5: Submit Spark Jobs ─────────────────────────────────────────────────
echo ""
echo "▶ Step 5: Submitting Spark analytics jobs..."
bash run_spark_jobs.sh

# ── Summary ───────────────────────────────────────────────────────────────────
echo ""
echo "╔══════════════════════════════════════════════════════════╗"
echo "║  UrbanPulse Task C — All Systems Running                 ║"
echo "╠══════════════════════════════════════════════════════════╣"
echo "║  Flink UI  : http://localhost:8081                       ║"
echo "║  Spark UI  : http://localhost:8080                       ║"
echo "╠══════════════════════════════════════════════════════════╣"
echo "║  Output Kafka topics:                                    ║"
echo "║    urbanpulse.incidents          (Flink alerts)          ║"
echo "║    urbanpulse.ward_energy_summary (Spark energy aggs)    ║"
echo "║    urbanpulse.health_advisories   (Spark AQI advisories) ║"
echo "║  Parquet: ./output/ward_energy/   (partitioned)          ║"
echo "╚══════════════════════════════════════════════════════════╝"
echo ""
echo "Smart Meter Producer PID: $PRODUCER_PID"
echo "To stop: ./stop.sh"
