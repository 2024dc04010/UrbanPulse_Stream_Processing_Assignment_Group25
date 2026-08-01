import csv
import json
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    key_serializer=lambda k: k.encode("utf-8"),
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

with open("route_schedule.csv", "r") as f:
    reader = csv.DictReader(f)

    for row in reader:
        route_id = row["route_id"]

        producer.send(
            "urbanpulse.route_schedule",
            key=route_id,
            value=row
        )

        print(f"Published route schedule: {row}")

producer.flush()
producer.close()

print("Route schedule KTable source topic populated.")
