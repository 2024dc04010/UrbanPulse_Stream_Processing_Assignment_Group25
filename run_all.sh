#!/bin/bash
# =============================================================================
# UrbanPulse — Run Everything in Parallel
# =============================================================================
# Starts the complete UrbanPulse pipeline end-to-end:
#
#   Phase 1 — Infrastructure
#     • Task C Docker services (Flink JM + TM + Spark)
#
#   Phase 2 — Kafka Topics
#     • Create Task B topics (if not already present)
#     • Create Task C output topics
#
#   Phase 3 — All Producers (background, logged)
#     • Task B: bus_gps_producer.py
#     • Task B: air_quality_producer.py
#     • Task B: traffic_signal_producer.py
#     • Task C: smart_meter_producer.py
#
#   Phase 4 — Task B Consumers + DLQ (background, logged)
#     • high_priority_consumer.py
#     • standard_priority_consumer.py
#     • dlq_processor.py
#
#   Phase 5 — Flink Jobs (submitted to container)
#     • aqi_emergency_detector.py
#     • gridlock_detector.py
#     • bunching_detector.py
#
#   Phase 6 — Spark Jobs (submitted to container)
#     • ward_energy_streaming.py
#     • health_advisories.py
#
# Prerequisites:
#   • Docker running
#   • Kafka broker accessible at localhost:9092
#     (start it however Task B expects — e.g. kafka-server-start.sh or Docker)
#   • pip install kafka-python==3.0.9
#
# Usage:
#   cd UrbanPulse_Stream_Processing_Assignment_Group25/
#   chmod +x run_all.sh
#   ./run_all.sh
#
# Logs: all/logs/<component>.log
# =============================================================================

set -e
ROOT_DIR="$(cd "$(dirname "$0")" && pwd)"
LOG_DIR="$ROOT_DIR/all/logs"
mkdir -p "$LOG_DIR"

TASKB="$ROOT_DIR/taskB"
TASKC="$ROOT_DIR/taskC"

# ─── colours ──────────────────────────────────────────────────────────────────
RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'
CYAN='\033[0;36m'; BOLD='\033[1m'; RESET='\033[0m'

log()  { echo -e "${CYAN}[$(date +%H:%M:%S)]${RESET} $*"; }
ok()   { echo -e "${GREEN}  ✔ $*${RESET}"; }
warn() { echo -e "${YELLOW}  ⚠ $*${RESET}"; }
err()  { echo -e "${RED}  ✖ $*${RESET}"; }

# Track PIDs for clean shutdown
PIDS=()

cleanup() {
    echo ""
    log "Shutting down all background processes..."
    for pid in "${PIDS[@]}"; do
        kill "$pid" 2>/dev/null && echo "  killed PID $pid" || true
    done
    log "Stopping Docker services..."
    cd "$TASKC" && docker compose down 2>/dev/null || true
    log "Done. Goodbye."
}
trap cleanup EXIT INT TERM

# =============================================================================
# PHASE 1 — Docker Services (Flink + Spark)
# =============================================================================
echo ""
echo -e "${BOLD}╔══════════════════════════════════════════════════════════╗${RESET}"
echo -e "${BOLD}║  UrbanPulse — Full Pipeline Startup                       ║${RESET}"
echo -e "${BOLD}╚══════════════════════════════════════════════════════════╝${RESET}"
echo ""
log "PHASE 1 — Starting Docker services (Flink + Spark)..."

cd "$TASKC"
docker compose up -d --build
ok "Docker services started."

# Wait for Flink JobManager UI
log "Waiting for Flink JobManager (http://localhost:8081)..."
for i in $(seq 1 60); do
    if curl -s http://localhost:8081/overview > /dev/null 2>&1; then
        ok "Flink JobManager is ready."
        break
    fi
    sleep 3
    echo -n "."
done
echo ""

# =============================================================================
# PHASE 2 — Kafka Topics
# =============================================================================
log "PHASE 2 — Creating Kafka topics..."

# Helper: create a topic if it doesn't exist (using kafka-python to check)
create_topic_if_missing() {
    local topic="$1" partitions="$2"
    python3 - <<PYEOF 2>/dev/null
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError
client = KafkaAdminClient(bootstrap_servers="localhost:9092", client_id="topic_creator")
try:
    client.create_topics([NewTopic(name="$topic", num_partitions=$partitions, replication_factor=1)])
    print("  Created: $topic")
except TopicAlreadyExistsError:
    print("  Exists:  $topic")
except Exception as e:
    print(f"  Error for $topic: {e}")
finally:
    client.close()
PYEOF
}

# Task B topics
create_topic_if_missing "urbanpulse.bus_gps"        12
create_topic_if_missing "urbanpulse.air_quality"    3
create_topic_if_missing "urbanpulse.traffic_signals" 6
create_topic_if_missing "urbanpulse.smart_meters"   8
create_topic_if_missing "urbanpulse.route_schedule"  3
create_topic_if_missing "urbanpulse.enriched_bus_gps" 3
create_topic_if_missing "urbanpulse.dlq"             1

# Task C topics
create_topic_if_missing "urbanpulse.incidents"           3
create_topic_if_missing "urbanpulse.ward_energy_summary" 3
create_topic_if_missing "urbanpulse.health_advisories"   3

ok "All Kafka topics are ready."

# =============================================================================
# PHASE 3 — Start All Producers (background)
# =============================================================================
log "PHASE 3 — Starting all Kafka producers..."

start_bg() {
    local name="$1" cmd="$2" logfile="$3"
    $cmd >> "$logfile" 2>&1 &
    local pid=$!
    PIDS+=($pid)
    ok "$name started (PID: $pid | log: $(basename "$logfile"))"
}

start_bg "Bus GPS Producer" \
    "python3 $TASKB/producers/bus_gps_producer.py" \
    "$LOG_DIR/bus_gps_producer.log"

start_bg "Air Quality Producer" \
    "python3 $TASKB/producers/air_quality_producer.py" \
    "$LOG_DIR/air_quality_producer.log"

start_bg "Traffic Signal Producer" \
    "python3 $TASKB/producers/traffic_signal_producer.py" \
    "$LOG_DIR/traffic_signal_producer.log"

start_bg "Smart Meter Producer" \
    "python3 $TASKC/smart_meter_producer.py" \
    "$LOG_DIR/smart_meter_producer.log"

sleep 2   # Give producers a moment to connect before consumers start

# =============================================================================
# PHASE 4 — Task B Consumers + DLQ (background)
# =============================================================================
log "PHASE 4 — Starting Task B consumers and DLQ processor..."

start_bg "High-Priority Consumer" \
    "python3 $TASKB/consumers/high_priority_consumer.py" \
    "$LOG_DIR/high_priority_consumer.log"

start_bg "Standard-Priority Consumer" \
    "python3 $TASKB/consumers/standard_priority_consumer.py" \
    "$LOG_DIR/standard_priority_consumer.log"

start_bg "DLQ Processor" \
    "python3 $TASKB/dlq/dlq_processor.py" \
    "$LOG_DIR/dlq_processor.log"

# =============================================================================
# PHASE 5 — Submit Flink Jobs
# =============================================================================
log "PHASE 5 — Submitting Flink incident detection jobs..."

FLINK_JM="urbanpulse-flink-jobmanager"

submit_flink() {
    local name="$1" script="$2"
    docker exec -d "$FLINK_JM" flink run -py "/opt/flink/jobs/$(basename "$script")"
    ok "Flink job submitted: $name"
    sleep 1
}

submit_flink "AQI Emergency Detector"  "$TASKC/flink/aqi_emergency_detector.py"
submit_flink "Traffic Gridlock Detector" "$TASKC/flink/gridlock_detector.py"
submit_flink "Bus Bunching Detector"   "$TASKC/flink/bunching_detector.py"

# =============================================================================
# PHASE 6 — Submit Spark Jobs
# =============================================================================
log "PHASE 6 — Submitting Spark analytics jobs..."

SPARK_CONTAINER="urbanpulse-spark"
KAFKA_PKG="org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0"

submit_spark() {
    local name="$1" script="$2"
    docker exec -d "$SPARK_CONTAINER" spark-submit \
        --master "local[2]" \
        --packages "$KAFKA_PKG" \
        --conf "spark.driver.memory=1g" \
        "/opt/spark/jobs/$(basename "$script")"
    ok "Spark job submitted: $name"
    sleep 2
}

submit_spark "Ward Energy Streaming"  "$TASKC/spark/ward_energy_streaming.py"
submit_spark "Health Advisories SQL"  "$TASKC/spark/health_advisories.py"

# =============================================================================
# Summary
# =============================================================================
echo ""
echo -e "${BOLD}╔══════════════════════════════════════════════════════════╗${RESET}"
echo -e "${BOLD}║  UrbanPulse — All Systems Running ✔                      ║${RESET}"
echo -e "${BOLD}╠══════════════════════════════════════════════════════════╣${RESET}"
echo -e "${BOLD}║  Flink UI    http://localhost:8081                        ║${RESET}"
echo -e "${BOLD}║  Spark UI    http://localhost:8080                        ║${RESET}"
echo -e "${BOLD}╠══════════════════════════════════════════════════════════╣${RESET}"
echo -e "${BOLD}║  Logs        ./all/logs/                                  ║${RESET}"
echo -e "${BOLD}║  Parquet     ./taskC/output/ward_energy/                  ║${RESET}"
echo -e "${BOLD}╚══════════════════════════════════════════════════════════╝${RESET}"
echo ""
echo -e "  Background PIDs: ${PIDS[*]}"
echo ""
echo -e "  Press ${BOLD}Ctrl+C${RESET} to stop everything."

# Keep the script alive (cleanup runs on EXIT)
wait
