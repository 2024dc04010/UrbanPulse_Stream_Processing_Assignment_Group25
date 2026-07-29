import json
import random
import time
from datetime import datetime
from kafka import KafkaProducer

# Producer configured with at-least-once delivery semantics
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    key_serializer=lambda k: k.encode('utf-8')
)

routes = ["R10", "R11", "R12", "R13"]

print("Starting Bus GPS Producer with Error Injection... (Press Ctrl+C to stop)")

while True:
    route_id = random.choice(routes)

    event = {
        "bus_id": f"BUS{random.randint(100, 999)}",
        "route_id": route_id,
        "lat": round(random.uniform(12.90, 13.10), 6),
        "lon": round(random.uniform(77.50, 77.70), 6),
        "speed_kmh": random.randint(0, 60),
        "occupancy_pct": random.randint(10, 100),
        "timestamp": datetime.utcnow().isoformat()
    }

    # Simulate 10% invalid GPS coordinates for fast DLQ testing
    if random.random() < 0.10:
        event["lat"] = 999.000000
        event["lon"] = 999.000000

    producer.send(
        "urbanpulse.bus_gps",
        key=route_id,
        value=event
    )

    print(f"Sent: {event}")
    time.sleep(1)
