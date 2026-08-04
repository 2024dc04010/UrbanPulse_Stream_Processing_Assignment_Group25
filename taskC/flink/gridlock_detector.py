"""
Task C — Part I (C1b): Flink Traffic Gridlock Detector
=======================================================
Assignment requirement:
    Traffic Gridlock: a junction's average wait time exceeds 180 seconds
    for 3 consecutive signal cycles → emit gridlock alert with
    junction_id and zone.

Design:
    - Source:    urbanpulse.traffic_signals
    - Key:       junction_id (keyed stream)
    - State:     ListState[int] — last 3 avg_wait_sec readings per junction
    - Watermark: Event-time with 10-second bounded out-of-orderness
    - Logic:     On each event, append avg_wait_sec. If all 3 most-recent
                 values exceed 180s → emit TRAFFIC_GRIDLOCK alert.
    - Output:    urbanpulse.incidents (JSON alert)

Run inside Flink container:
    flink run -py /opt/flink/jobs/gridlock_detector.py
"""

import json
import logging
from datetime import datetime, timezone

from pyflink.common import WatermarkStrategy, Duration, Types
from pyflink.common.watermark_strategy import TimestampAssigner
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import (
    KafkaSource,
    KafkaSink,
    KafkaRecordSerializationSchema,
    KafkaOffsetResetStrategy,
)
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream.functions import KeyedProcessFunction, RuntimeContext
from pyflink.datastream.state import ListStateDescriptor, ValueStateDescriptor

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("GridlockDetector")

# ── Configuration ────────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP       = "host.docker.internal:9092"
SOURCE_TOPIC          = "urbanpulse.traffic_signals"
OUTPUT_TOPIC          = "urbanpulse.incidents"
GRIDLOCK_THRESHOLD_S  = 180     # seconds — average wait time threshold
CONSECUTIVE_CYCLES    = 3       # number of consecutive cycles needed
ALERT_COOLDOWN_MS     = 5 * 60 * 1000   # 5-minute per-junction cooldown

# ── Timestamp Assigner ───────────────────────────────────────────────────────

class SignalTimestampAssigner(TimestampAssigner):
    """Extracts event-time from the traffic signal 'timestamp' field."""

    def extract_timestamp(self, value: str, record_timestamp: int) -> int:
        try:
            event = json.loads(value)
            ts_str = event.get("timestamp", "")
            dt = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
            return int(dt.timestamp() * 1000)
        except Exception:
            return record_timestamp

# ── KeyedProcessFunction ─────────────────────────────────────────────────────

class GridlockDetector(KeyedProcessFunction):
    """
    Keyed by junction_id. Tracks the last CONSECUTIVE_CYCLES avg_wait_sec
    values using ListState. When all stored values exceed GRIDLOCK_THRESHOLD_S,
    a TRAFFIC_GRIDLOCK alert is emitted.

    A per-junction cooldown (ALERT_COOLDOWN_MS) prevents repeated alerts
    during a sustained gridlock event.
    """

    def __init__(self):
        self._wait_history   = None   # ListState[int]: rolling window of wait times
        self._last_alert_ms  = None   # ValueState[long]: timestamp of last alert

    def open(self, runtime_context: RuntimeContext):
        self._wait_history = runtime_context.get_list_state(
            ListStateDescriptor("wait_history", Types.INT())
        )
        self._last_alert_ms = runtime_context.get_state(
            ValueStateDescriptor("last_alert_ms", Types.LONG())
        )

    def process_element(self, value: str, ctx: "KeyedProcessFunction.Context"):
        try:
            event     = json.loads(value)
            wait_sec  = event.get("avg_wait_sec")
            junction  = event.get("junction_id", ctx.get_current_key())
            zone      = event.get("zone", "Unknown")

            if wait_sec is None:
                return

            # Append current reading to history
            self._wait_history.add(int(wait_sec))

            # Retrieve history and keep only last CONSECUTIVE_CYCLES entries
            history = list(self._wait_history.get())
            if len(history) > CONSECUTIVE_CYCLES:
                history = history[-CONSECUTIVE_CYCLES:]
                self._wait_history.update(history)

            # Log state for observability
            logger.info(
                f"Junction {junction} | wait history: {history}"
            )

            # Need exactly CONSECUTIVE_CYCLES readings before checking
            if len(history) < CONSECUTIVE_CYCLES:
                return

            # Check if all consecutive readings exceed threshold
            if not all(w > GRIDLOCK_THRESHOLD_S for w in history):
                return

            # Apply cooldown: don't re-alert the same junction within cooldown window
            current_ms = ctx.timestamp() or int(
                datetime.now(timezone.utc).timestamp() * 1000
            )
            last_alert = self._last_alert_ms.value()
            if last_alert is not None and (current_ms - last_alert) < ALERT_COOLDOWN_MS:
                return

            self._last_alert_ms.update(current_ms)

            alert = {
                "alert_type":              "TRAFFIC_GRIDLOCK",
                "junction_id":             junction,
                "zone":                    zone,
                "consecutive_readings_sec": history,
                "consecutive_cycles":      CONSECUTIVE_CYCLES,
                "min_wait_sec":            min(history),
                "max_wait_sec":            max(history),
                "threshold_sec":          GRIDLOCK_THRESHOLD_S,
                "event_timestamp":         event.get("timestamp"),
                "alert_generated_at":      datetime.now(timezone.utc)
                                           .strftime("%Y-%m-%dT%H:%M:%SZ"),
            }

            logger.warning(
                f"[ALERT] TRAFFIC_GRIDLOCK | junction={junction} "
                f"| zone={zone} | readings={history}"
            )
            yield json.dumps(alert)

        except Exception as exc:
            logger.error(f"Error in GridlockDetector: {exc} | raw={value}")

# ── Main ─────────────────────────────────────────────────────────────────────

def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    # ── Watermark strategy: event-time with 10s out-of-order tolerance ────
    watermark_strategy = (
        WatermarkStrategy
        .for_bounded_out_of_orderness(Duration.of_seconds(10))
        .with_timestamp_assigner(SignalTimestampAssigner())
    )

    # ── Kafka source ───────────────────────────────────────────────────────
    source = (
        KafkaSource.builder()
        .set_bootstrap_servers(KAFKA_BOOTSTRAP)
        .set_topics(SOURCE_TOPIC)
        .set_group_id("flink-gridlock-group")
        .set_starting_offsets(KafkaOffsetResetStrategy.LATEST)
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
    stream = env.from_source(source, watermark_strategy, "Traffic Signal Source")

    alerts = (
        stream
        .key_by(
            lambda raw: json.loads(raw).get("junction_id", "unknown"),
            key_type=Types.STRING()
        )
        .process(GridlockDetector(), output_type=Types.STRING())
    )

    alerts.sink_to(sink)

    logger.info(
        f"Starting Traffic Gridlock Detector | "
        f"source={SOURCE_TOPIC} | sink={OUTPUT_TOPIC} | "
        f"threshold={GRIDLOCK_THRESHOLD_S}s | cycles={CONSECUTIVE_CYCLES}"
    )
    env.execute("UrbanPulse — Traffic Gridlock Detector")


if __name__ == "__main__":
    main()
