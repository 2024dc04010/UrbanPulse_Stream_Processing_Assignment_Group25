import json

import time

from kafka import KafkaConsumer


consumer = KafkaConsumer(

    "urbanpulse.traffic_signals",

    bootstrap_servers="localhost:9092",

    group_id="analytics_dashboard_group",

    auto_offset_reset="latest",

    value_deserializer=lambda m: json.loads(m.decode("utf-8"))

)


print("STANDARD_PRIORITY Consumer Running")


for msg in consumer:

    event = msg.value


    print(

        f"[STANDARD_PRIORITY] "

        f"junction={event['junction_id']} "

        f"vehicles={event['vehicle_count']} "

        f"phase={event['signal_phase']}"

    )


    # Simulated slow analytics workload

    time.sleep(5)
 
