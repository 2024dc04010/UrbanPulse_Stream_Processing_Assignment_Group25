import csv

import json

from kafka import KafkaConsumer, KafkaProducer


# Load route lookup table

route_lookup = {}


with open("route_schedule.csv", "r") as f:

    reader = csv.DictReader(f)

    for row in reader:

        route_lookup[row["route_id"]] = row


consumer = KafkaConsumer(

    "urbanpulse.bus_gps",

    bootstrap_servers="localhost:9092",

    group_id="route_enrichment_group",

    auto_offset_reset="latest",

    value_deserializer=lambda m: json.loads(m.decode("utf-8"))

)


producer = KafkaProducer(

    bootstrap_servers="localhost:9092",

    value_serializer=lambda v: json.dumps(v).encode("utf-8")

)


print("Route enrichment service running...")


for msg in consumer:


    gps_event = msg.value


    route_id = gps_event.get("route_id")


    route_info = route_lookup.get(route_id)


    if route_info:


        enriched_event = {

            **gps_event,

            "route_name": route_info["route_name"],

            "terminal": route_info["terminal"],

            "scheduled_arrival": route_info["scheduled_arrival"]

        }


        producer.send(

            "urbanpulse.enriched_bus_gps",

            value=enriched_event

        )


        producer.flush()


        print(enriched_event)
