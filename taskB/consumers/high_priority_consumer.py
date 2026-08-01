import json

from kafka import KafkaConsumer


consumer = KafkaConsumer(

    "urbanpulse.traffic_signals",

    bootstrap_servers="localhost:9092",

    group_id="signal_control_group",

    auto_offset_reset="latest",

    value_deserializer=lambda m: json.loads(m.decode("utf-8"))

)


print("HIGH_PRIORITY Consumer Running")


for msg in consumer:

    event = msg.value


    print(

        f"[HIGH_PRIORITY] "

        f"junction={event['junction_id']} "

        f"vehicles={event['vehicle_count']} "

        f"phase={event['signal_phase']}"

    )
