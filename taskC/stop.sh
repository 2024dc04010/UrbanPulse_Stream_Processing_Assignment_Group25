#!/bin/bash
# ============================================================
# Task C — Stop all services
# ============================================================
cd "$(dirname "$0")"

echo "Stopping Docker services..."
docker compose down

echo "Killing any background Python producers..."
pkill -f smart_meter_producer.py 2>/dev/null || true

echo "All Task C services stopped."
