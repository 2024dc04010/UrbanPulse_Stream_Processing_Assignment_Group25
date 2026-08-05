"""
Task C — Part I (C1a): Flink AQI Emergency Detector
=====================================================
Assignment requirement:
    AQI Emergency: any air quality sensor reports AQI > 300 (Hazardous)
    → emit alert within 2 minutes of the reading
    → output to urbanpulse.incidents Kafka topic

Design:
    - Source:     urbanpulse.air_quality
    - Key:        sensor_id (keyed stream)
    - State:      ValueState storing last-emitted alert timestamp to avoid
                  duplicate alerts for the same sensor within 2 minutes
    - Watermark:  Event-time with 10-second bounded out-of-orderness
    - Output:     urbanpulse.incidents (JSON alert)

Run inside Flink container:
    flink run -py /opt/flink/jobs/aqi_emergency_detector.py
"""

import json
import logging
import math
import os
from datetime import datetime, timezone

from pyflink.common import WatermarkStrategy, Duration, Types
from pyflink.common.watermark_strategy import TimestampAssigner
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import (
    KafkaSource,
    KafkaSink,
    KafkaRecordSerializationSchema,
    KafkaOffsetsInitializer,
)
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream.functions import KeyedProcessFunction, RuntimeContext
from pyflink.datastream.state import ValueStateDescriptor

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("AQIEmergencyDetector")

# ── Configuration ────────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
SOURCE_TOPIC    = "urbanpulse.air_quality"
OUTPUT_TOPIC    = "urbanpulse.incidents"
AQI_THRESHOLD   = 300          # Hazardous threshold
ALERT_COOLDOWN_MS = 2 * 60 * 1000   # 2-minute cooldown per sensor

# ── Timestamp Assigner ───────────────────────────────────────────────────────

class AQITimestampAssigner(TimestampAssigner):
    """
    Extracts event-time timestamp from the 'timestamp' field of the
    air quality JSON event. Falls back to processing time on parse error.
    """

    def extract_timestamp(self, value: str, record_timestamp: int) -> int:
        try:
            event = json.loads(value)
            ts_str = event.get("timestamp", "")
            dt = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
            return int(dt.timestamp() * 1000)
        except Exception:
            return record_timestamp

# ── KeyedProcessFunction ─────────────────────────────────────────────────────

class AQIEmergencyDetector(KeyedProcessFunction):
    """
    Keyed by sensor_id. Emits an AQI_EMERGENCY alert whenever the sensor
    reports AQI > 300. A per-sensor cooldown (ALERT_COOLDOWN_MS) prevents
    flooding the incidents topic with repeated alerts from the same sensor.
    """

    def __init__(self):
        self._last_alert_time = None   # ValueState[long]: ms timestamp of last alert

    def open(self, runtime_context: RuntimeContext):
        descriptor = ValueStateDescriptor("last_alert_time_ms", Types.LONG())
        self._last_alert_time = runtime_context.get_state(descriptor)

    def process_element(self, value: str, ctx: "KeyedProcessFunction.Context"):
        try:
            event = json.loads(value)
            aqi   = event.get("aqi")

            # Only process valid AQI > 300
            if aqi is None or not isinstance(aqi, (int, float)):
                return
            if aqi <= AQI_THRESHOLD:
                return

            current_ms = ctx.timestamp() or int(datetime.now(timezone.utc).timestamp() * 1000)
            last_alert = self._last_alert_time.value()

            # Suppress duplicate alerts within the cooldown window
            if last_alert is not None and (current_ms - last_alert) < ALERT_COOLDOWN_MS:
                return

            self._last_alert_time.update(current_ms)

            alert = {
                "alert_type":         "AQI_EMERGENCY",
                "sensor_id":          event.get("sensor_id"),
                "zone":               event.get("zone"),
                "aqi":                aqi,
                "pm25":               event.get("pm25"),
                "pm10":               event.get("pm10"),
                "no2":                event.get("no2"),
                "event_timestamp":    event.get("timestamp"),
                "alert_generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
                "severity":           "CRITICAL" if aqi > 400 else "HAZARDOUS",
            }

            logger.warning(
                f"[ALERT] AQI_EMERGENCY | sensor={alert['sensor_id']} "
                f"| zone={alert['zone']} | aqi={aqi}"
            )
            yield json.dumps(alert)

        except Exception as exc:
            logger.error(f"Error processing event: {exc} | raw={value}")

# ── Main ─────────────────────────────────────────────────────────────────────

def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    # Automatically load the Kafka connector JAR if running natively
    jar_path = os.path.abspath(os.path.join(os.path.dirname(__file__), 'flink-sql-connector-kafka-3.0.2-1.18.jar'))
    if os.path.exists(jar_path):
        env.add_jars(f"file://{jar_path}")

    # ── Watermark strategy: event-time with 10s out-of-order tolerance ────
    watermark_strategy = (
        WatermarkStrategy
        .for_bounded_out_of_orderness(Duration.of_seconds(10))
        .with_timestamp_assigner(AQITimestampAssigner())
    )

    # ── Kafka source ───────────────────────────────────────────────────────
    source = (
        KafkaSource.builder()
        .set_bootstrap_servers(KAFKA_BOOTSTRAP)
        .set_topics(SOURCE_TOPIC)
        .set_group_id("flink-aqi-emergency-group")
        .set_starting_offsets(KafkaOffsetsInitializer.latest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )

    # ── Kafka sink ─────────────────────────────────────────────────────────
    sink = (
        KafkaSink.builder()
        .set_bootstrap_servers(KAFKA_BOOTSTRAP)
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic(OUTPUT_TOPIC)
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
        )
        .build()
    )

    # ── Pipeline ───────────────────────────────────────────────────────────
    stream = env.from_source(source, watermark_strategy, "AQI Source")

    alerts = (
        stream
        .key_by(lambda raw: json.loads(raw).get("sensor_id", "unknown"),
                key_type=Types.STRING())
        .process(AQIEmergencyDetector(), output_type=Types.STRING())
    )

    alerts.sink_to(sink)

    logger.info(f"Starting AQI Emergency Detector | "
                f"source={SOURCE_TOPIC} | sink={OUTPUT_TOPIC} | threshold={AQI_THRESHOLD}")
    env.execute("UrbanPulse — AQI Emergency Detector")


if __name__ == "__main__":
    main()
