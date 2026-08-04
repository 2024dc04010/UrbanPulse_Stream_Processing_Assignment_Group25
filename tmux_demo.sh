#!/bin/bash
# =============================================================================
# UrbanPulse — tmux Demo Session
# =============================================================================
# Creates a fully split tmux window where you can watch every component's
# live output simultaneously. Perfect for the video walkthrough.
#
# Layout:
#
#  ┌─────────────────────┬─────────────────────┬─────────────────────┐
#  │  Bus GPS Producer   │  Air Quality Prod.  │ Traffic Signal Prod │
#  │  (taskB)            │  (taskB)            │  (taskB)            │
#  ├─────────────────────┼─────────────────────┼─────────────────────┤
#  │  Smart Meter Prod   │  High-Prio Consumer │  DLQ Processor      │
#  │  (taskC)            │  (taskB)            │  (taskB)            │
#  ├─────────────────────┼─────────────────────┼─────────────────────┤
#  │  Flink Incidents    │  Spark Ward Energy  │  Spark Advisories   │
#  │  (kafka consumer)   │  (kafka consumer)   │  (kafka consumer)   │
#  └─────────────────────┴─────────────────────┴─────────────────────┘
#
# Usage:
#   chmod +x tmux_demo.sh
#   ./tmux_demo.sh
#
# Requires: tmux   (brew install tmux)
# =============================================================================

set -e
ROOT_DIR="$(cd "$(dirname "$0")" && pwd)"
TASKB="$ROOT_DIR/taskB"
TASKC="$ROOT_DIR/taskC"

SESSION="urbanpulse"

# ── Colours for pane titles ───────────────────────────────────────────────────
KAFKA_CONSUMER="kafka-console-consumer.sh --bootstrap-server localhost:9092 --from-beginning --topic"

# Kill existing session if present
tmux kill-session -t "$SESSION" 2>/dev/null || true

# ── Create session with 3×3 grid ─────────────────────────────────────────────
tmux new-session -d -s "$SESSION" -x 220 -y 55

# ── Rename first window ───────────────────────────────────────────────────────
tmux rename-window -t "$SESSION:0" "UrbanPulse Live"

# ── Row 1: Producers ─────────────────────────────────────────────────────────

# Pane 0 (top-left): Bus GPS Producer
tmux send-keys -t "$SESSION:0" \
    "echo '=== Bus GPS Producer ===' && python3 $TASKB/producers/bus_gps_producer.py" Enter

# Pane 1 (top-centre): Air Quality Producer
tmux split-window -h -t "$SESSION:0"
tmux send-keys -t "$SESSION:0.1" \
    "echo '=== Air Quality Producer ===' && python3 $TASKB/producers/air_quality_producer.py" Enter

# Pane 2 (top-right): Traffic Signal Producer
tmux split-window -h -t "$SESSION:0.1"
tmux send-keys -t "$SESSION:0.2" \
    "echo '=== Traffic Signal Producer ===' && python3 $TASKB/producers/traffic_signal_producer.py" Enter

# ── Row 2: Consumers + Smart Meter ───────────────────────────────────────────

# Pane 3 (mid-left): Smart Meter Producer (Task C)
tmux select-pane -t "$SESSION:0.0"
tmux split-window -v -t "$SESSION:0.0"
tmux send-keys -t "$SESSION:0.3" \
    "echo '=== Smart Meter Producer (Task C) ===' && python3 $TASKC/smart_meter_producer.py" Enter

# Pane 4 (mid-centre): High Priority Consumer
tmux select-pane -t "$SESSION:0.1"
tmux split-window -v -t "$SESSION:0.1"
tmux send-keys -t "$SESSION:0.4" \
    "echo '=== HIGH PRIORITY Consumer ===' && python3 $TASKB/consumers/high_priority_consumer.py" Enter

# Pane 5 (mid-right): DLQ Processor
tmux select-pane -t "$SESSION:0.2"
tmux split-window -v -t "$SESSION:0.2"
tmux send-keys -t "$SESSION:0.5" \
    "echo '=== DLQ Processor ===' && python3 $TASKB/dlq/dlq_processor.py" Enter

# ── Row 3: Kafka output topic tails ──────────────────────────────────────────

# Pane 6 (bot-left): Flink Incidents output
tmux select-pane -t "$SESSION:0.3"
tmux split-window -v -t "$SESSION:0.3"
tmux send-keys -t "$SESSION:0.6" \
    "sleep 10 && echo '=== Flink Incidents ===' && $KAFKA_CONSUMER urbanpulse.incidents" Enter

# Pane 7 (bot-centre): Spark Ward Energy output
tmux select-pane -t "$SESSION:0.4"
tmux split-window -v -t "$SESSION:0.4"
tmux send-keys -t "$SESSION:0.7" \
    "sleep 10 && echo '=== Spark Ward Energy ===' && $KAFKA_CONSUMER urbanpulse.ward_energy_summary" Enter

# Pane 8 (bot-right): Spark Health Advisories output
tmux select-pane -t "$SESSION:0.5"
tmux split-window -v -t "$SESSION:0.5"
tmux send-keys -t "$SESSION:0.8" \
    "sleep 10 && echo '=== Health Advisories ===' && $KAFKA_CONSUMER urbanpulse.health_advisories" Enter

# ── Even out the layout ───────────────────────────────────────────────────────
tmux select-layout -t "$SESSION:0" tiled

# ── Second window: Flink + Spark UIs ─────────────────────────────────────────
tmux new-window -t "$SESSION" -n "Infra Logs"

# Left: Flink TaskManager logs
tmux send-keys -t "$SESSION:1" \
    "echo '=== Flink TaskManager Logs ===' && docker logs -f urbanpulse-flink-taskmanager 2>&1" Enter

# Right: Spark logs
tmux split-window -h -t "$SESSION:1"
tmux send-keys -t "$SESSION:1.1" \
    "echo '=== Spark Container Logs ===' && docker logs -f urbanpulse-spark 2>&1" Enter

# ── Focus on the main window ─────────────────────────────────────────────────
tmux select-window -t "$SESSION:0"
tmux select-pane   -t "$SESSION:0.0"

# ── Attach ────────────────────────────────────────────────────────────────────
echo ""
echo "╔══════════════════════════════════════════════════════╗"
echo "║  UrbanPulse tmux session starting...                 ║"
echo "║                                                      ║"
echo "║  Navigation:                                         ║"
echo "║    Ctrl+B →   next window                           ║"
echo "║    Ctrl+B ←   prev window                           ║"
echo "║    Ctrl+B z   zoom current pane                     ║"
echo "║    Ctrl+B d   detach (session keeps running)        ║"
echo "║    Ctrl+B &   kill window                           ║"
echo "╚══════════════════════════════════════════════════════╝"
echo ""
echo "  Flink UI  →  http://localhost:8081"
echo "  Spark UI  →  http://localhost:8080"
echo ""

tmux attach-session -t "$SESSION"
