import json
from collections import Counter
from kafka import KafkaConsumer

print("Connecting to Kafka to aggregate DLQ metrics...")

# Using a new consumer group_id forces Kafka to read from earliest offset
consumer = KafkaConsumer(
    "urbanpulse.dlq",
    bootstrap_servers="localhost:9092",
    group_id="dlq_report_group_run2",
    auto_offset_reset="earliest",
    consumer_timeout_ms=10000,  # Auto-closes after 10s of inactivity
    value_deserializer=lambda m: json.loads(m.decode("utf-8"))
)

counter = Counter()

for msg in consumer:
    event = msg.value
    error_reason = event.get("error_reason", "UNKNOWN")
    counter[error_reason] += 1

print("\n" + "=" * 35)
print("   DLQ ERROR DISTRIBUTION REPORT   ")
print("=" * 35)

if not counter:
    print(" No DLQ messages found.")
else:
    for error, count in counter.items():
        print(f" {error:<25} : {count}")

print("=" * 35 + "\n")

