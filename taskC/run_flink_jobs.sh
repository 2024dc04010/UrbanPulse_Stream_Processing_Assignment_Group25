#!/bin/bash
# ============================================================
# Task C — Submit all 3 Flink incident detection jobs
#
# Prerequisites:
#   1. docker compose up (Flink services healthy)
#   2. Task B Kafka running at localhost:9092
#   3. Task B producers running (air_quality, traffic_signals, bus_gps)
#
# Usage:
#   chmod +x run_flink_jobs.sh
#   ./run_flink_jobs.sh
# ============================================================

set -e

JOBMANAGER="urbanpulse-flink-jobmanager"
FLINK_CMD="docker exec $JOBMANAGER flink run -py"

echo "============================================================"
echo " UrbanPulse Task C — Flink Job Submission"
echo "============================================================"

# Wait for JobManager to be healthy
echo "Waiting for Flink JobManager to be ready..."
until docker exec $JOBMANAGER curl -s http://localhost:8081/overview > /dev/null 2>&1; do
    sleep 2
    echo "  ... still waiting"
done
echo "Flink JobManager is ready."
echo ""

# ── C1a: AQI Emergency Detector ──────────────────────────────────────────────
echo "[1/3] Submitting AQI Emergency Detector..."
docker exec -d $JOBMANAGER flink run \
    -py /opt/flink/jobs/aqi_emergency_detector.py \
    -pn "AQI Emergency Detector"
echo "      → Submitted: AQI Emergency Detector"
sleep 2

# ── C1b: Traffic Gridlock Detector ───────────────────────────────────────────
echo "[2/3] Submitting Traffic Gridlock Detector..."
docker exec -d $JOBMANAGER flink run \
    -py /opt/flink/jobs/gridlock_detector.py \
    -pn "Traffic Gridlock Detector"
echo "      → Submitted: Traffic Gridlock Detector"
sleep 2

# ── C1c: Bus Bunching Detector ────────────────────────────────────────────────
echo "[3/3] Submitting Bus Bunching Detector..."
docker exec -d $JOBMANAGER flink run \
    -py /opt/flink/jobs/bunching_detector.py \
    -pn "Bus Bunching Detector"
echo "      → Submitted: Bus Bunching Detector"
sleep 2

echo ""
echo "All 3 Flink jobs submitted."
echo "View running jobs at: http://localhost:8081"
echo ""

# List running jobs
echo "Current Flink jobs:"
docker exec $JOBMANAGER flink list -r 2>/dev/null || true
