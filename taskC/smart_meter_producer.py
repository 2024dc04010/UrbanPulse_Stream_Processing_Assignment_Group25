"""
Task C — Smart Meter Producer
==============================
Produces smart meter energy consumption events into:
    urbanpulse.smart_meters

Event Schema:
    {
        "meter_id":     "M0042",
        "ward_id":      "W03",
        "kwh_consumed": 1.84,
        "power_factor": 0.93,
        "voltage":      231.2,
        "timestamp":    "2026-08-02T16:00:00Z"
    }

This producer was not included in Task B. Task C's Spark
ward energy streaming job consumes from this topic.
"""

import json
import random
import time
import logging
from datetime import datetime, timezone
from kafka import KafkaProducer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# ── Kafka configuration ──────────────────────────────────────────────────────
producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    acks="all",
    retries=3
)

# ── Simulation parameters ────────────────────────────────────────────────────
WARDS = [f"W{str(i).zfill(2)}" for i in range(1, 11)]   # W01 – W10
METERS_PER_WARD = 5   # 5 meters per ward → 50 meters total

def generate_meter_id(ward_id: str, meter_num: int) -> str:
    ward_num = ward_id.replace("W", "")
    return f"M{ward_num}{meter_num:02d}"

def generate_event(ward_id: str, meter_num: int) -> dict:
    meter_id = generate_meter_id(ward_id, meter_num)

    # Simulate realistic energy consumption (higher during peak hours 8–20)
    hour = datetime.now(timezone.utc).hour
    is_peak = 8 <= hour < 20
    base_kwh = random.uniform(0.8, 2.5) if is_peak else random.uniform(0.2, 1.0)

    return {
        "meter_id":     meter_id,
        "ward_id":      ward_id,
        "kwh_consumed": round(base_kwh, 3),
        "power_factor": round(random.uniform(0.70, 0.99), 3),
        "voltage":      round(random.uniform(218.0, 242.0), 2),
        "timestamp":    datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    }

print("Starting Smart Meter Producer... (Press Ctrl+C to stop)")
logger.info("Smart Meter Producer started. Publishing to urbanpulse.smart_meters")

event_count = 0

try:
    while True:
        # Emit one event per meter per cycle (50 events every ~5 seconds)
        for ward_id in WARDS:
            for meter_num in range(1, METERS_PER_WARD + 1):
                event = generate_event(ward_id, meter_num)
                producer.send("urbanpulse.smart_meters", value=event)
                event_count += 1

        producer.flush()
        logger.info(f"Published {METERS_PER_WARD * len(WARDS)} smart meter events "
                    f"| total={event_count}")
        print(f"[Smart Meters] Published batch | total events: {event_count}")

        time.sleep(5)   # emit every 5 seconds

except KeyboardInterrupt:
    logger.info("Smart Meter Producer stopped by user.")
    print("\nStopping Smart Meter Producer.")
finally:
    producer.close()
