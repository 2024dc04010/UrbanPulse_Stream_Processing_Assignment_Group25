#!/bin/bash
# =============================================================================
# UrbanPulse — Linux Server All-In-One Startup Script
# =============================================================================
# Run this once on the Linux server to start the complete pipeline:
#
#   1. Kafka (ZooKeeper + Broker)  — started as background processes
#   2. All Kafka topics             — created via kafka-topics.sh
#   3. Task B Producers             — bus_gps, air_quality, traffic_signals
#   4. Task C Smart Meter Producer  — smart_meters
#   5. Task B Consumers + DLQ       — high_priority, standard_priority, dlq_processor
#   6. Docker: Flink + Spark        — docker compose up
#   7. Flink Jobs                   — 3 incident detectors submitted to cluster
#   8. Spark Jobs                   — ward energy + health advisories submitted
#
# Usage:
#   cd UrbanPulse_Stream_Processing_Assignment_Group25/
#   chmod +x server_run.sh
#   ./server_run.sh
#
# To stop:
#   ./server_stop.sh
#
# Logs: ./all/logs/
# =============================================================================

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")" && pwd)"
TASKB="$ROOT_DIR/taskB"
TASKC="$ROOT_DIR/taskC"
LOG_DIR="$ROOT_DIR/all/logs"
PID_FILE="$ROOT_DIR/all/pids.txt"
KAFKA_DIR="$ROOT_DIR/kafka_2.13-3.9.1"
KAFKA_BOOTSTRAP="localhost:9092"

mkdir -p "$LOG_DIR"
> "$PID_FILE"   # reset PID file on each run

# ── Colours ───────────────────────────────────────────────────────────────────
GREEN='\033[0;32m'; YELLOW='\033[1;33m'; CYAN='\033[0;36m'
RED='\033[0;31m'; BOLD='\033[1m'; RESET='\033[0m'

log()  { echo -e "${CYAN}[$(date '+%H:%M:%S')]${RESET} $*"; }
ok()   { echo -e "${GREEN}  ✔ $*${RESET}"; }
warn() { echo -e "${YELLOW}  ⚠ $*${RESET}"; }
fail() { echo -e "${RED}  ✖ $*${RESET}"; exit 1; }

# ── Background process helper (saves PID for server_stop.sh) ─────────────────
run_bg() {
    local name="$1"; shift
    local logfile="$LOG_DIR/${name// /_}.log"
    nohup "$@" >> "$logfile" 2>&1 &
    local pid=$!
    echo "$pid $name" >> "$PID_FILE"
    ok "$name  (PID=$pid  log=$(basename "$logfile"))"
    sleep 1
}

# ── Wait for TCP port to be open ─────────────────────────────────────────────
wait_for_port() {
    local host="$1" port="$2" desc="$3" max="${4:-60}"
    log "Waiting for $desc ($host:$port)..."
    for i in $(seq 1 "$max"); do
        if bash -c "echo > /dev/tcp/$host/$port" 2>/dev/null; then
            ok "$desc is ready."
            return 0
        fi
        sleep 2
    done
    fail "$desc did not start within $((max*2)) seconds."
}

echo ""
echo -e "${BOLD}╔══════════════════════════════════════════════════════════════╗${RESET}"
echo -e "${BOLD}║   UrbanPulse — Linux Server Full Pipeline Startup            ║${RESET}"
echo -e "${BOLD}╚══════════════════════════════════════════════════════════════╝${RESET}"
echo ""

# =============================================================================
# PHASE 1 — Kafka
# =============================================================================
log "PHASE 1 — Starting Kafka..."

# Check if Kafka is already up
if bash -c "echo > /dev/tcp/localhost/9092" 2>/dev/null; then
    warn "Kafka already running at localhost:9092 — skipping start."
else
    if [[ ! -d "$KAFKA_DIR" ]]; then
        fail "Kafka dir not found: $KAFKA_DIR. Start Kafka manually and re-run."
    fi

    log "Starting ZooKeeper..."
    run_bg "zookeeper" \
        "$KAFKA_DIR/bin/zookeeper-server-start.sh" \
        "$KAFKA_DIR/config/zookeeper.properties"

    wait_for_port localhost 2181 "ZooKeeper" 30

    log "Starting Kafka broker..."
    run_bg "kafka_broker" \
        "$KAFKA_DIR/bin/kafka-server-start.sh" \
        "$KAFKA_DIR/config/server.properties"

    wait_for_port localhost 9092 "Kafka Broker" 30
fi

# =============================================================================
# PHASE 2 — Kafka Topics
# =============================================================================
log "PHASE 2 — Creating all Kafka topics..."

KAFKA_TOPICS="$KAFKA_DIR/bin/kafka-topics.sh"

create_topic() {
    local topic="$1" partitions="$2"
    "$KAFKA_TOPICS" --create \
        --topic "$topic" \
        --partitions "$partitions" \
        --replication-factor 1 \
        --bootstrap-server "$KAFKA_BOOTSTRAP" \
        --if-not-exists 2>/dev/null \
    && echo "  [topic] $topic"
}

# Task B topics
create_topic "urbanpulse.bus_gps"           12
create_topic "urbanpulse.air_quality"       3
create_topic "urbanpulse.traffic_signals"   6
create_topic "urbanpulse.smart_meters"      8
create_topic "urbanpulse.route_schedule"    3
create_topic "urbanpulse.enriched_bus_gps"  3
create_topic "urbanpulse.dlq"               1

# Task C topics
create_topic "urbanpulse.incidents"             3
create_topic "urbanpulse.ward_energy_summary"   3
create_topic "urbanpulse.health_advisories"     3

ok "All Kafka topics ready."

# Set retention policies (Task B requirement)
log "Applying retention policies..."
"$KAFKA_DIR/bin/kafka-configs.sh" \
    --bootstrap-server "$KAFKA_BOOTSTRAP" \
    --entity-type topics --entity-name urbanpulse.bus_gps \
    --alter --add-config retention.ms=86400000    2>/dev/null || true   # 24h

"$KAFKA_DIR/bin/kafka-configs.sh" \
    --bootstrap-server "$KAFKA_BOOTSTRAP" \
    --entity-type topics --entity-name urbanpulse.air_quality \
    --alter --add-config retention.ms=7776000000  2>/dev/null || true   # 90d

"$KAFKA_DIR/bin/kafka-configs.sh" \
    --bootstrap-server "$KAFKA_BOOTSTRAP" \
    --entity-type topics --entity-name urbanpulse.smart_meters \
    --alter --add-config retention.ms=31536000000 2>/dev/null || true   # 365d

"$KAFKA_DIR/bin/kafka-configs.sh" \
    --bootstrap-server "$KAFKA_BOOTSTRAP" \
    --entity-type topics --entity-name urbanpulse.traffic_signals \
    --alter --add-config retention.ms=604800000   2>/dev/null || true   # 7d

"$KAFKA_DIR/bin/kafka-configs.sh" \
    --bootstrap-server "$KAFKA_BOOTSTRAP" \
    --entity-type topics --entity-name urbanpulse.dlq \
    --alter --add-config retention.ms=2592000000  2>/dev/null || true   # 30d
ok "Retention policies applied."

# =============================================================================
# PHASE 3 — Task B Producers
# =============================================================================
log "PHASE 3 — Starting Task B producers..."

run_bg "bus_gps_producer"       python3 "$TASKB/producers/bus_gps_producer.py"
run_bg "air_quality_producer"   python3 "$TASKB/producers/air_quality_producer.py"
run_bg "traffic_signal_producer" python3 "$TASKB/producers/traffic_signal_producer.py"

# =============================================================================
# PHASE 4 — Task C Smart Meter Producer
# =============================================================================
log "PHASE 4 — Starting smart meter producer..."
run_bg "smart_meter_producer"   python3 "$TASKC/smart_meter_producer.py"

# =============================================================================
# PHASE 5 — Task B Consumers + DLQ
# =============================================================================
log "PHASE 5 — Starting Task B consumers and DLQ processor..."

run_bg "high_priority_consumer"     python3 "$TASKB/consumers/high_priority_consumer.py"
run_bg "standard_priority_consumer" python3 "$TASKB/consumers/standard_priority_consumer.py"
run_bg "dlq_processor"              python3 "$TASKB/dlq/dlq_processor.py"

# =============================================================================
# PHASE 6 — Install Python dependencies and Flink JAR
# =============================================================================
log "PHASE 6 — Preparing Flink and Spark native execution..."

log "Installing PyFlink and PySpark (may take a minute)..."
pip3 install --quiet apache-flink==1.18.1 pyspark==3.5.0
ok "Python streaming libraries installed."

FLINK_JAR="flink-sql-connector-kafka-3.0.2-1.18.jar"
if [[ ! -f "$TASKC/flink/$FLINK_JAR" ]]; then
    log "Downloading Flink Kafka connector..."
    curl -sSLo "$TASKC/flink/$FLINK_JAR" "https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.0.2-1.18/$FLINK_JAR"
    ok "Downloaded $FLINK_JAR."
fi

export KAFKA_BOOTSTRAP="$KAFKA_BOOTSTRAP"

# =============================================================================
# PHASE 7 — Submit Flink Jobs
# =============================================================================
log "PHASE 7 — Submitting Flink incident detection jobs (Native)..."

# Note: Flink mini-cluster starts automatically when executed via python3
run_bg "flink_aqi_detector"       python3 "$TASKC/flink/aqi_emergency_detector.py"
run_bg "flink_gridlock_detector"  python3 "$TASKC/flink/gridlock_detector.py"
run_bg "flink_bunching_detector"  python3 "$TASKC/flink/bunching_detector.py"

# =============================================================================
# PHASE 8 — Submit Spark Jobs
# =============================================================================
log "PHASE 8 — Submitting Spark analytics jobs (Native)..."

export PYSPARK_PYTHON=python3
export PYSPARK_DRIVER_PYTHON=python3

run_bg "spark_ward_energy"    python3 "$TASKC/spark/ward_energy_streaming.py"
run_bg "spark_health_advisory" python3 "$TASKC/spark/health_advisories.py"

# =============================================================================
# Summary
# =============================================================================
echo ""
echo -e "${BOLD}╔══════════════════════════════════════════════════════════════╗${RESET}"
echo -e "${BOLD}║   UrbanPulse — All Systems Running ✔                         ║${RESET}"
echo -e "${BOLD}╠══════════════════════════════════════════════════════════════╣${RESET}"
echo -e "${BOLD}║   Execution Mode: Native (No Docker)                          ║${RESET}"
echo -e "${BOLD}║   Kafka UI: none (running natively in background)             ║${RESET}"
echo -e "${BOLD}╠══════════════════════════════════════════════════════════════╣${RESET}"
echo -e "${BOLD}║   Logs          ./all/logs/                                   ║${RESET}"
echo -e "${BOLD}║   PIDs          ./all/pids.txt                                ║${RESET}"
echo -e "${BOLD}║   Parquet out   ./taskC/output/ward_energy/                   ║${RESET}"
echo -e "${BOLD}╠══════════════════════════════════════════════════════════════╣${RESET}"
echo -e "${BOLD}║   To stop:      ./server_stop.sh                              ║${RESET}"
echo -e "${BOLD}╚══════════════════════════════════════════════════════════════╝${RESET}"
echo ""
echo "Background processes:"
cat "$PID_FILE"
