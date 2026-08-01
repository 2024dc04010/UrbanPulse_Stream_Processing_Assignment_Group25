import json

import random

import time

from datetime import datetime

from kafka import KafkaProducer


producer = KafkaProducer(

    bootstrap_servers="localhost:9092",

    value_serializer=lambda v: json.dumps(v).encode("utf-8")

)


zones = ["North", "South", "East", "West"]


print("Traffic Signal Producer Started")


while True:

    event = {

        "junction_id": f"J{random.randint(1, 20)}",

        "zone": random.choice(zones),

        "vehicle_count": random.randint(10, 300),

        "avg_wait_sec": random.randint(20, 250),

        "signal_phase": random.choice(["GREEN", "RED", "YELLOW"]),

        "timestamp": datetime.utcnow().isoformat()

    }


    producer.send(

        "urbanpulse.traffic_signals",

        value=event

    )


    producer.flush()


    print(event)


    time.sleep(1)
