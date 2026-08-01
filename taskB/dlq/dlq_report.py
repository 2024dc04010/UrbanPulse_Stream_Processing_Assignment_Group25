import json
import time
from collections import Counter
from kafka import KafkaConsumer

REPORT_DURATION_SECONDS = 300

print("Connecting to Kafka to aggregate DLQ metrics...")

print(f"Collecting DLQ messages for {REPORT_DURATION_SECONDS} seconds...")

group_name = "dlq_report_group_" + str(int(time.time()))

consumer = KafkaConsumer(

    "urbanpulse.dlq",

    bootstrap_servers="localhost:9092",

    group_id=group_name,

    auto_offset_reset="earliest",

    enable_auto_commit=False,

    value_deserializer=lambda m: json.loads(m.decode("utf-8"))

)


counter = Counter()
start_time = time.time()
while time.time() - start_time < REPORT_DURATION_SECONDS:

    records = consumer.poll(timeout_ms=1000)


    for topic_partition, messages in records.items():

        for msg in messages:

            event = msg.value

            error_reason = event.get("error_reason", "UNKNOWN")

            counter[error_reason] += 1


consumer.close()

print("\n" + "=" * 45)

print("DLQ ERROR DISTRIBUTION REPORT")

print("=" * 45)

print(f"Report window: {REPORT_DURATION_SECONDS} seconds")


if not counter:

    print("No DLQ messages found.")

else:

    for error, count in counter.items():

        print(f"{error:<30} : {count}")


print("=" * 45)
