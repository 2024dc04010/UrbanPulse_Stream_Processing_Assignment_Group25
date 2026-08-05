#!/bin/bash
# =============================================================================
# UrbanPulse — Linux Server Stop Script
# Kills all background processes started by server_run.sh and stops Docker.
# =============================================================================

ROOT_DIR="$(cd "$(dirname "$0")" && pwd)"
TASKC="$ROOT_DIR/taskC"
PID_FILE="$ROOT_DIR/all/pids.txt"
KAFKA_DIR="$ROOT_DIR/kafka_2.13-3.9.1"

GREEN='\033[0;32m'; CYAN='\033[0;36m'; RESET='\033[0m'
log() { echo -e "${CYAN}[$(date '+%H:%M:%S')]${RESET} $*"; }
ok()  { echo -e "${GREEN}  ✔ $*${RESET}"; }

echo ""
log "Stopping UrbanPulse pipeline..."

# Kill all tracked background PIDs
if [[ -f "$PID_FILE" ]]; then
    while IFS=' ' read -r pid name; do
        if kill -0 "$pid" 2>/dev/null; then
            kill "$pid" && ok "Stopped: $name (PID $pid)"
        else
            echo "  Already stopped: $name (PID $pid)"
        fi
    done < "$PID_FILE"
    > "$PID_FILE"
else
    echo "  No PID file found — nothing tracked to stop."
fi

# Stop Kafka (optional — comment out if you want Kafka to keep running)
log "Stopping Kafka broker and ZooKeeper..."
"$KAFKA_DIR/bin/kafka-server-stop.sh"  2>/dev/null || true
sleep 2
"$KAFKA_DIR/bin/zookeeper-server-stop.sh" 2>/dev/null || true
ok "Kafka stopped."

echo ""
ok "All UrbanPulse services stopped."
