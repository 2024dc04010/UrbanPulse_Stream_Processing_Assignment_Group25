import json
import random
import time
import logging
from datetime import datetime
from kafka import KafkaProducer

# Configure logging output
logging.basicConfig(
    filename="air_quality.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# Producer configured with at-least-once delivery semantics
producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    acks="all",
    retries=5,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

zones = ["North", "South", "East", "West", "Central"]

print("Starting Air Quality Producer with Error Injection... (Press Ctrl+C to stop)")

while True:
    aqi = random.randint(20, 400)

    # Simulate 10% out-of-range AQI values for DLQ testing
    if random.random() < 0.10:
        aqi = random.choice([-10, 999])
        logging.warning("Out-of-range AQI generated for DLQ testing.")

    # Simulate 10% null AQI values for DLQ testing
    if random.random() < 0.10:
        aqi = None
        logging.warning("Null AQI generated and handled gracefully.")

    event = {
        "sensor_id": f"AQ{random.randint(100, 999)}",
        "zone": random.choice(zones),
        "pm25": round(random.uniform(10, 250), 2),
        "pm10": round(random.uniform(20, 300), 2),
        "no2": round(random.uniform(5, 150), 2),
        "aqi": aqi,
        "timestamp": datetime.utcnow().isoformat()
    }

    producer.send(
        "urbanpulse.air_quality",
        value=event
    )

    producer.flush()
    print(event)
    time.sleep(1)

