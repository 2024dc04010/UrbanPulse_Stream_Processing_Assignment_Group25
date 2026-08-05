"""
Task C — Part I (C1c): Flink Bus Bunching Detector
===================================================
Assignment requirement:
    Bus Bunching: two buses on the same route_id are within 200 metres
    of each other for more than 5 minutes → emit bunching alert with
    both bus IDs.

Design:
    - Source:    urbanpulse.bus_gps
    - Key:       route_id (keyed stream — all buses on a route processed together)
    - State:
        * MapState[bus_id → JSON position]  latest lat/lon per bus
        * MapState[pair_key → first_close_ms]  when pair first came within 200m
    - Watermark: Event-time with 30-second bounded out-of-orderness (GPS can
                 arrive slightly late on congested networks)
    - Logic:     On each GPS event:
                 1. Update position for this bus
                 2. Compute Haversine distance to every other bus on the route
                 3. If distance < 200m → record/check "close since" timestamp
                 4. If close for > 5 minutes → emit BUS_BUNCHING alert
                 5. If distance ≥ 200m → clear the tracked pair
    - Output:    urbanpulse.incidents (JSON alert)

Run inside Flink container:
    flink run -py /opt/flink/jobs/bunching_detector.py
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
from pyflink.datastream.state import MapStateDescriptor

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("BusBunchingDetector")

# ── Configuration ────────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP    = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
SOURCE_TOPIC       = "urbanpulse.bus_gps"
OUTPUT_TOPIC       = "urbanpulse.incidents"
BUNCHING_DIST_M    = 200          # distance threshold in metres
BUNCHING_DURATION_MS = 5 * 60 * 1000   # 5 minutes in milliseconds
ALERT_COOLDOWN_MS  = 5 * 60 * 1000     # 5-minute cooldown per pair

# ── Haversine Distance ───────────────────────────────────────────────────────

def haversine_metres(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """
    Returns the great-circle distance in metres between two (lat, lon) points.
    Uses the Haversine formula.
    """
    R = 6_371_000   # Earth radius in metres
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    d_phi   = math.radians(lat2 - lat1)
    d_lam   = math.radians(lon2 - lon1)
    a = (math.sin(d_phi / 2) ** 2
         + math.cos(phi1) * math.cos(phi2) * math.sin(d_lam / 2) ** 2)
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))

# ── Timestamp Assigner ───────────────────────────────────────────────────────

class GPSTimestampAssigner(TimestampAssigner):
    """Extracts event-time from the bus GPS 'timestamp' field."""

    def extract_timestamp(self, value: str, record_timestamp: int) -> int:
        try:
            event = json.loads(value)
            ts_str = event.get("timestamp", "")
            dt = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
            return int(dt.timestamp() * 1000)
        except Exception:
            return record_timestamp

# ── KeyedProcessFunction ─────────────────────────────────────────────────────

class BusBunchingDetector(KeyedProcessFunction):
    """
    Keyed by route_id. Tracks the position of every bus on the route using
    MapState. When any two buses are within BUNCHING_DIST_M for more than
    BUNCHING_DURATION_MS, a BUS_BUNCHING alert is emitted.

    State:
        bus_positions  : MapState[bus_id  → JSON{"lat", "lon"}]
        close_pairs    : MapState[pair_key → first_close_timestamp_ms]
        alerted_pairs  : MapState[pair_key → last_alert_timestamp_ms]
    """

    def __init__(self):
        self._bus_positions  = None
        self._close_pairs    = None
        self._alerted_pairs  = None

    def open(self, runtime_context: RuntimeContext):
        self._bus_positions = runtime_context.get_map_state(
            MapStateDescriptor("bus_positions",  Types.STRING(), Types.STRING())
        )
        self._close_pairs = runtime_context.get_map_state(
            MapStateDescriptor("close_pairs",    Types.STRING(), Types.LONG())
        )
        self._alerted_pairs = runtime_context.get_map_state(
            MapStateDescriptor("alerted_pairs",  Types.STRING(), Types.LONG())
        )

    def process_element(self, value: str, ctx: "KeyedProcessFunction.Context"):
        try:
            event    = json.loads(value)
            bus_id   = event.get("bus_id")
            route_id = event.get("route_id")
            lat      = event.get("lat")
            lon      = event.get("lon")

            if not all([bus_id, route_id, lat is not None, lon is not None]):
                return

            # Validate GPS coordinates (DLQ-rejected events may still arrive)
            if not (-90 <= lat <= 90 and -180 <= lon <= 180):
                return

            current_ms = ctx.timestamp() or int(
                datetime.now(timezone.utc).timestamp() * 1000
            )

            # 1. Snapshot all other buses before updating current position
            other_buses = {}
            for entry in self._bus_positions.items():
                other_id  = entry.get_key()
                other_pos = json.loads(entry.get_value())
                if other_id != bus_id:
                    other_buses[other_id] = other_pos

            # 2. Update this bus's position
            self._bus_positions.put(bus_id, json.dumps({"lat": lat, "lon": lon}))

            # 3. Check distance to every other bus on the same route
            for other_id, other_pos in other_buses.items():
                dist_m = haversine_metres(
                    lat, lon, other_pos["lat"], other_pos["lon"]
                )
                pair_key = "_".join(sorted([bus_id, other_id]))

                if dist_m < BUNCHING_DIST_M:
                    # Buses are close — record or check the pair
                    if not self._close_pairs.contains(pair_key):
                        # First time they're within 200m
                        self._close_pairs.put(pair_key, current_ms)
                        logger.info(
                            f"Route {route_id}: buses {bus_id} & {other_id} "
                            f"are {dist_m:.0f}m apart — tracking..."
                        )
                    else:
                        first_close_ms = self._close_pairs.get(pair_key)
                        duration_ms    = current_ms - first_close_ms

                        if duration_ms >= BUNCHING_DURATION_MS:
                            # Check cooldown for this pair
                            last_alerted = (self._alerted_pairs.get(pair_key)
                                            if self._alerted_pairs.contains(pair_key)
                                            else None)
                            if (last_alerted is None or
                                    (current_ms - last_alerted) >= ALERT_COOLDOWN_MS):

                                self._alerted_pairs.put(pair_key, current_ms)

                                alert = {
                                    "alert_type":      "BUS_BUNCHING",
                                    "route_id":        route_id,
                                    "bus_1":           sorted([bus_id, other_id])[0],
                                    "bus_2":           sorted([bus_id, other_id])[1],
                                    "distance_m":      round(dist_m, 2),
                                    "duration_min":    round(duration_ms / 60_000, 2),
                                    "threshold_m":     BUNCHING_DIST_M,
                                    "event_timestamp": event.get("timestamp"),
                                    "alert_generated_at": datetime.now(timezone.utc)
                                                          .strftime("%Y-%m-%dT%H:%M:%SZ"),
                                }

                                logger.warning(
                                    f"[ALERT] BUS_BUNCHING | route={route_id} "
                                    f"| {bus_id} & {other_id} | dist={dist_m:.0f}m "
                                    f"| duration={duration_ms/60000:.1f}min"
                                )
                                yield json.dumps(alert)
                else:
                    # Buses are no longer bunched — reset tracking
                    if self._close_pairs.contains(pair_key):
                        self._close_pairs.remove(pair_key)
                        logger.info(
                            f"Route {route_id}: pair {pair_key} separated "
                            f"({dist_m:.0f}m) — cleared"
                        )

        except Exception as exc:
            logger.error(f"Error in BusBunchingDetector: {exc} | raw={value}")

# ── Main ─────────────────────────────────────────────────────────────────────

def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    # Automatically load the Kafka connector JAR if running natively
    jar_path = os.path.abspath(os.path.join(os.path.dirname(__file__), 'flink-sql-connector-kafka-3.0.2-1.18.jar'))
    if os.path.exists(jar_path):
        env.add_jars(f"file://{jar_path}")

    # ── Watermark strategy: 30s tolerance for GPS network latency ─────────
    watermark_strategy = (
        WatermarkStrategy
        .for_bounded_out_of_orderness(Duration.of_seconds(30))
        .with_timestamp_assigner(GPSTimestampAssigner())
    )

    # ── Kafka source ───────────────────────────────────────────────────────
    source = (
        KafkaSource.builder()
        .set_bootstrap_servers(KAFKA_BOOTSTRAP)
        .set_topics(SOURCE_TOPIC)
        .set_group_id("flink-bunching-group")
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
    stream = env.from_source(source, watermark_strategy, "Bus GPS Source")

    alerts = (
        stream
        .key_by(
            lambda raw: json.loads(raw).get("route_id", "unknown"),
            key_type=Types.STRING()
        )
        .process(BusBunchingDetector(), output_type=Types.STRING())
    )

    alerts.sink_to(sink)

    logger.info(
        f"Starting Bus Bunching Detector | "
        f"source={SOURCE_TOPIC} | sink={OUTPUT_TOPIC} | "
        f"dist_threshold={BUNCHING_DIST_M}m | duration={BUNCHING_DURATION_MS/60000:.0f}min"
    )
    env.execute("UrbanPulse — Bus Bunching Detector")


if __name__ == "__main__":
    main()
