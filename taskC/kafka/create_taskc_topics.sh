#!/bin/bash
# ============================================================
# Task C — Create Kafka topics for Flink and Spark output
# Run this after Task B's Kafka cluster is up.
# ============================================================

BOOTSTRAP_SERVER="localhost:9092"

echo "Creating Task C Kafka output topics..."

# Flink incident alerts (output from all 3 Flink detectors)
kafka-topics.sh --create \
    --topic urbanpulse.incidents \
    --partitions 3 \
    --replication-factor 1 \
    --bootstrap-server $BOOTSTRAP_SERVER \
    --if-not-exists

# Spark ward energy summary (output from Spark Structured Streaming)
kafka-topics.sh --create \
    --topic urbanpulse.ward_energy_summary \
    --partitions 3 \
    --replication-factor 1 \
    --bootstrap-server $BOOTSTRAP_SERVER \
    --if-not-exists

# Spark health advisories (output from Streaming SQL job)
kafka-topics.sh --create \
    --topic urbanpulse.health_advisories \
    --partitions 3 \
    --replication-factor 1 \
    --bootstrap-server $BOOTSTRAP_SERVER \
    --if-not-exists

echo ""
echo "Verifying Task C topics:"
kafka-topics.sh --list --bootstrap-server $BOOTSTRAP_SERVER | grep urbanpulse

echo ""
echo "Done. Task C topics are ready."
