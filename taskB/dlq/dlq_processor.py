import json
import logging
from kafka import KafkaConsumer, KafkaProducer

# Configure logging output
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# Multi-topic consumer
consumer = KafkaConsumer(
    "urbanpulse.air_quality",
    "urbanpulse.bus_gps",
    bootstrap_servers="localhost:9092",
    group_id="dlq_validator_group",
    auto_offset_reset="earliest",
    value_deserializer=lambda m: json.loads(m.decode("utf-8"))
)

# Producer for DLQ routing
producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

print("DLQ Processor running...")
print("Listening on: urbanpulse.air_quality and urbanpulse.bus_gps\n")


def validate_air_quality(event):
    """Validates Air Quality events for missing or out-of-bound AQI values."""
    aqi = event.get("aqi")
    
    if aqi is None:
        return "NULL_AQI"
    if aqi < 0 or aqi > 500:
        return "AQI_OUT_OF_RANGE"
    
    return None


def validate_bus_gps(event):
    """Validates Bus GPS events for missing or impossible GPS coordinates."""
    lat = event.get("lat")
    lon = event.get("lon")
    
    if lat is None or lon is None:
        return "NULL_GPS_COORDINATE"
    if lat < -90 or lat > 90 or lon < -180 or lon > 180:
        return "INVALID_GPS_COORDINATES"
    
    return None


# Processing loop
for msg in consumer:
    event = msg.value
    error_reason = None

    if msg.topic == "urbanpulse.air_quality":
        error_reason = validate_air_quality(event)
    elif msg.topic == "urbanpulse.bus_gps":
        error_reason = validate_bus_gps(event)

    if error_reason:
        dlq_event = {
            "error_reason": error_reason,
            "source_topic": msg.topic,
            "partition": msg.partition,
            "offset": msg.offset,
            "original_event": event
        }

        # Send to Dead Letter Queue
        producer.send("urbanpulse.dlq", value=dlq_event)
        producer.flush()

        logging.warning(
            f"Routed to DLQ | reason={error_reason} | topic={msg.topic} | offset={msg.offset}"
        )
        print(f"[DLQ ROUTED] {dlq_event}\n")
    else:
        logging.info(f"Valid event from {msg.topic}")
