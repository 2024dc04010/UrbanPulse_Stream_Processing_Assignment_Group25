# Task C — Flink Real-Time Incident Detection & Spark Urban Analytics Engine

## UrbanPulse — Real-Time Urban Operations Intelligence Platform

### Course
Stream Processing and Analytics

### Domain
Smart Cities & Urban Infrastructure | 35 Marks

---

# Overview

Task C implements the processing layer of the UrbanPulse platform. It comprises
two independent engines deployed on top of the Apache Kafka ingestion layer from
Task B:

1. **Apache Flink** — sub-2-minute incident detection pipeline
2. **Apache Spark Structured Streaming** — 15-minute ward-level analytics
   engine feeding the councillor dashboard

Both engines consume from the Kafka topics created in Task B and produce output
to new Kafka topics and a Parquet data lake.

---

# Architecture

```
 Task B Kafka Topics (localhost:9092)
 ────────────────────────────────────────────────────────────────────
 urbanpulse.air_quality     ──► [Flink] AQI Emergency Detector  ──┐
 urbanpulse.traffic_signals ──► [Flink] Gridlock Detector        ├─► urbanpulse.incidents
 urbanpulse.bus_gps         ──► [Flink] Bus Bunching Detector   ──┘

 urbanpulse.smart_meters    ──► [Spark] Ward Energy Streaming ──► urbanpulse.ward_energy_summary
                                                              └──► Parquet: /output/ward_energy/

 urbanpulse.air_quality     ──► [Spark SQL] Health Advisories ──► urbanpulse.health_advisories
                                    + static zone_profile.csv
```

---

# Contents

| Problem Statement | Deliverable | Marks |
|---|---|---|
| 1. Flink — 3 incident patterns | AQI Emergency, Traffic Gridlock, Bus Bunching with keyed state & event-time watermarks | 10 |
| 2. Spark — ward energy 15-min tumbling window | 45-min watermark, dual output (Kafka + Parquet) | 20 |
| 3. Flink vs Spark comparison | 4 dimensions addressed with UrbanPulse-specific data | 5 |
| **Total** | | **35** |

---

# Part I — Apache Flink: Incident Detection (10 marks)

## Design Principles

All three Flink jobs share the same architectural pattern:

| Component | Choice | Rationale |
|---|---|---|
| Time characteristic | Event-time | Sensors timestamp events at source; processing-time watermarks would create false alerts if events arrive slightly late |
| Watermark strategy | `for_bounded_out_of_orderness` | GPS / sensor events can arrive 10–30 seconds out of order on congested city networks |
| State backend | FileSystem (flink-checkpoints Docker volume) | Survives TaskManager restarts; checkpointed every 10 s |
| Keying strategy | Per-entity key (sensor_id, junction_id, route_id) | Ensures all events for the same entity are processed by the same task slot, making state consistent |
| Output | `urbanpulse.incidents` Kafka topic | Shared alert bus consumed by the dashboard and notification services |

---

## C1a — AQI Emergency Detector (`flink/aqi_emergency_detector.py`)

**Source**: `urbanpulse.air_quality`
**Key**: `sensor_id`

### Detection Logic

```
IF aqi > 300 (Hazardous — US EPA breakpoint)
THEN emit AQI_EMERGENCY alert immediately (within 2 minutes of event timestamp)
```

### Keyed State Used

| State | Type | Purpose |
|---|---|---|
| `last_alert_time_ms` | `ValueState[long]` | 2-minute per-sensor cooldown — prevents alert flooding from the same sensor during sustained hazardous conditions |

### Watermark

`WatermarkStrategy.for_bounded_out_of_orderness(Duration.of_seconds(10))`

Since AQI sensors report every second and network jitter is low, a 10-second
out-of-order tolerance is sufficient.

### Sample Alert Output

```json
{
  "alert_type": "AQI_EMERGENCY",
  "sensor_id": "AQ412",
  "zone": "North",
  "aqi": 347,
  "pm25": 185.4,
  "pm10": 220.1,
  "no2": 98.2,
  "event_timestamp": "2026-08-02T16:30:42Z",
  "alert_generated_at": "2026-08-02T16:30:43Z",
  "severity": "HAZARDOUS"
}
```

---

## C1b — Traffic Gridlock Detector (`flink/gridlock_detector.py`)

**Source**: `urbanpulse.traffic_signals`
**Key**: `junction_id`

### Detection Logic

```
Maintain a sliding window of the last 3 avg_wait_sec readings per junction.
IF all 3 consecutive readings > 180 seconds
THEN emit TRAFFIC_GRIDLOCK alert with junction_id and zone.
```

### Keyed State Used

| State | Type | Purpose |
|---|---|---|
| `wait_history` | `ListState[int]` | Stores the last 3 `avg_wait_sec` values for the junction; capped at 3 entries by explicit truncation on each event |
| `last_alert_ms` | `ValueState[long]` | 5-minute cooldown per junction to suppress repeated alerts during a sustained gridlock event |

### Watermark

`WatermarkStrategy.for_bounded_out_of_orderness(Duration.of_seconds(10))`

Traffic signal controllers emit events in near-real-time; 10 seconds is
sufficient to handle any queuing delay.

### Sample Alert Output

```json
{
  "alert_type": "TRAFFIC_GRIDLOCK",
  "junction_id": "J07",
  "zone": "East",
  "consecutive_readings_sec": [212, 198, 235],
  "consecutive_cycles": 3,
  "min_wait_sec": 198,
  "max_wait_sec": 235,
  "threshold_sec": 180,
  "event_timestamp": "2026-08-02T16:31:05Z",
  "alert_generated_at": "2026-08-02T16:31:06Z"
}
```

---

## C1c — Bus Bunching Detector (`flink/bunching_detector.py`)

**Source**: `urbanpulse.bus_gps`
**Key**: `route_id`

### Detection Logic

```
For every GPS event from bus B on route R:
  1. Update B's position in bus_positions MapState
  2. For every other bus B' on route R (from MapState):
     a. Compute Haversine distance(B, B')
     b. IF distance < 200 m:
          Record the pair in close_pairs with current timestamp (first_close_ms)
          IF (current_time - first_close_ms) >= 5 minutes:
             Emit BUS_BUNCHING alert (with 5-min per-pair cooldown)
     c. IF distance >= 200 m:
          Remove pair from close_pairs (buses separated — reset tracking)
```

### Keyed State Used

| State | Type | Purpose |
|---|---|---|
| `bus_positions` | `MapState[bus_id → JSON]` | Latest lat/lon for every bus currently tracked on the route |
| `close_pairs` | `MapState[pair_key → long]` | Timestamp (ms) when each pair of buses first came within 200 m |
| `alerted_pairs` | `MapState[pair_key → long]` | Timestamp of last alert per pair — enforces 5-minute cooldown |

`pair_key` = alphabetically sorted bus IDs joined by `_` (e.g. `BUS101_BUS203`)
to ensure the same pair is tracked regardless of which bus triggers the event.

### Distance Calculation

Haversine formula — correct for spherical Earth at city-scale distances
(< 1 km). The UrbanPulse simulation uses Bangalore coordinates
(lat ≈ 12.9–13.1, lon ≈ 77.5–77.7).

### Watermark

`WatermarkStrategy.for_bounded_out_of_orderness(Duration.of_seconds(30))`

GPS trackers may batch events over 3G/4G; 30 seconds accommodates the
longest observed transmission gap in the simulation.

### Sample Alert Output

```json
{
  "alert_type": "BUS_BUNCHING",
  "route_id": "R12",
  "bus_1": "BUS203",
  "bus_2": "BUS417",
  "distance_m": 143.7,
  "duration_min": 5.2,
  "threshold_m": 200,
  "event_timestamp": "2026-08-02T16:35:18Z",
  "alert_generated_at": "2026-08-02T16:35:19Z"
}
```

---

# Part II — Spark Structured Streaming: Ward Analytics (20 marks)

## C2 — Ward Energy 15-Minute Tumbling Window (`spark/ward_energy_streaming.py`)

**Source**: `urbanpulse.smart_meters`
**New producer**: `taskC/smart_meter_producer.py`

Task B documented the `urbanpulse.smart_meters` topic but did not include a
producer. Task C provides `smart_meter_producer.py`, which emits 50 events
every 5 seconds (10 wards × 5 meters per ward).

### Smart Meter Event Schema

```json
{
  "meter_id":     "M0101",
  "ward_id":      "W01",
  "kwh_consumed": 1.84,
  "power_factor": 0.93,
  "voltage":      231.2,
  "timestamp":    "2026-08-02T16:00:00Z"
}
```

### Window Specification

| Parameter | Value | Rationale |
|---|---|---|
| Window type | Tumbling | Non-overlapping; each kWh is counted once per ward per window |
| Window size | 15 minutes | Matches councillor dashboard refresh cadence |
| Late data watermark | **45 minutes** | Smart meters in basement/underground sites can buffer up to 30 min; 45 min provides a safety margin while bounding state size |
| Output mode | Append | Emits a final result per (ward_id, window) only after the watermark has passed — guarantees completeness before publishing |

### Aggregations Per Window Per Ward

| Metric | Spark Function | Meaning |
|---|---|---|
| `total_kwh_consumed` | `sum(kwh_consumed)` | Total energy consumed in the ward in the 15-min window |
| `avg_power_factor` | `avg(power_factor)` | Average power quality — sustained < 0.85 triggers utility alerts |
| `peak_voltage` | `max(voltage)` | Maximum recorded voltage — high values indicate grid instability |

### Dual Sink

**Sink 1 — Kafka** (`urbanpulse.ward_energy_summary`):

```json
{
  "ward_id": "W03",
  "window_start": "2026-08-02T16:00:00",
  "window_end":   "2026-08-02T16:15:00",
  "total_kwh_consumed": 47.82,
  "avg_power_factor":   0.91,
  "peak_voltage":       238.4
}
```

**Sink 2 — Parquet** (`/output/ward_energy/`, partitioned by `ward_id` and `date`):

```text
/output/ward_energy/
  ward_id=W01/date=2026-08-02/part-00000.snappy.parquet
  ward_id=W02/date=2026-08-02/part-00000.snappy.parquet
  ward_id=W03/date=2026-08-02/part-00000.snappy.parquet
  ...
```

Partitioning by `ward_id` + `date` allows councillors and auditors to query
a single ward's historical energy profile efficiently via Trino/Presto (as
specified in the Task A Kappa architecture).

---

## C3 — Streaming SQL: AQI Health Advisories (`spark/health_advisories.py`)

**Source**: `urbanpulse.air_quality`
**Static join**: `data/zone_profile.csv`

### Streaming SQL Query Logic

```
(a) 10-Minute Rolling Average AQI per Zone:
    SELECT zone, window(event_time, "10 minutes", "1 minute"),
           avg(aqi) AS rolling_avg_aqi
    FROM aqi_stream
    WHERE aqi BETWEEN 0 AND 500    -- exclude null/out-of-range events
    GROUP BY zone, window(...)

(b) Enrich with zone_profile (streaming–static join):
    SELECT r.*, z.zone_name, z.population, z.num_schools, z.num_hospitals
    FROM rolling_aqi r
    LEFT JOIN zone_profile z ON r.zone = z.zone

(c) Filter for Unhealthy AQI:
    WHERE rolling_avg_aqi > 150    -- US EPA Unhealthy threshold
```

### Output Mode: Update

The `update` output mode is required here because:
- The rolling average for a zone changes with every new event
- We want to emit the latest advisory whenever a zone's rolling average updates
- `update` emits the current state of each group that changed in the last trigger
- This is not possible with `append` (which only emits final results) or `complete`
  (which would re-emit all zones every trigger)

### Sample Advisory Output

```json
{
  "zone": "North",
  "zone_name": "North Zone",
  "population": 125000,
  "num_schools": 12,
  "num_hospitals": 3,
  "window_start": "2026-08-02T16:20:00",
  "window_end":   "2026-08-02T16:30:00",
  "rolling_avg_aqi": 187.4,
  "rolling_avg_pm25": 95.2,
  "rolling_avg_pm10": 130.8,
  "advisory_level": "UNHEALTHY",
  "advisory_message": "Sensitive groups should avoid outdoor activity. General public: limit exposure."
}
```

---

# Part III — Flink vs Spark: UrbanPulse Use-Case Mapping (5 marks)

## Summary Verdict

| Use Case | Recommended Engine | Reason |
|---|---|---|
| Bus Bunching Detection | **Flink** | Per-pair state tracking, true streaming, sub-second latency |
| Ward Energy Aggregation | **Spark** | Tumbling windows, SQL expressiveness, Parquet/Kafka dual output |

---

## Dimension 1: State Size

### Bus Bunching Detection (Flink)

The bunching detector maintains `MapState` for every bus on every route. In
the UrbanPulse simulation there are 4 routes (`R10–R13`) and many buses.
In production (city-scale), this could be hundreds of buses per route.

**Flink** manages this with RocksDB state backend (configured for production),
which spills state to disk and can handle state sizes exceeding available RAM.
Flink's keyed state is co-located with processing — the state for route `R12`
lives in the same task slot that processes `R12` events, so there is no
network overhead to fetch state.

**Spark** micro-batch processes the entire stream; implementing the same
position-tracking logic in Spark requires `mapGroupsWithState` or
`flatMapGroupsWithState`, which carries significantly higher programming
complexity and stores state in the driver's heap — a bottleneck at city scale.

**Winner: Flink** — native keyed state with disk-spillable backend is more
appropriate for large, long-lived per-entity state.

---

### Ward Energy Aggregation (Spark)

Each 15-minute window accumulates partial sums (`sum`, `avg`, `max`) for
10 wards. The state per ward per window is tiny (three `double` values).
Spark's in-memory micro-batch state is sufficient and requires no tuning.

**Flink** could handle this, but managing a tumbling window with a 45-minute
late-data watermark in PyFlink requires explicit `WindowAssigner` and
`WindowFunction` implementations — considerably more code than Spark's
`groupBy(window(...)).agg(...)`.

**Winner: Spark** — small aggregate state fits entirely in memory; Spark SQL
window functions provide a declarative, concise implementation.

---

## Dimension 2: Latency Requirement

### Bus Bunching Detection — SLA: < 2 minutes

The UrbanPulse operational SLA requires that bus bunching alerts reach the
transit control room within 2 minutes of the event occurring. This is the
time window for dispatchers to intervene (e.g., instruct the leading bus to
pause at the next stop).

**Flink** operates in true streaming mode. Each GPS event is processed
individually as it arrives. With a 30-second watermark tolerance and 1-second
producer interval, a bunching alert is emitted within ≈ 35 seconds of the
5-minute bunching condition being met — well within the 2-minute SLA.

**Spark** micro-batches at configurable intervals (typically 1–30 seconds).
Even at 1-second trigger intervals, state management overhead increases
latency significantly. More importantly, detecting a 5-minute duration
condition in Spark requires maintaining group state across many micro-batches —
each micro-batch must load state, apply the new event, and checkpoint updated
state, introducing cumulative overhead.

**Winner: Flink** — true event-by-event processing meets the sub-2-minute SLA
without latency accumulation from micro-batch overhead.

---

### Ward Energy Aggregation — SLA: 15-minute windows

The ward energy SLA is defined by the window itself: councillors receive
updated dashboards after each 15-minute window closes. There is no sub-minute
latency requirement; the watermark already introduces a deliberate 45-minute
delay to guarantee completeness.

**Spark** micro-batch at 15-minute trigger intervals is a natural fit. The
trigger fires once per window, computes the aggregation, and flushes to both
Kafka and Parquet in a single operation.

**Flink** would also work, but the added operational complexity of Flink's
window API, combined with the need to configure a dual Kafka+Parquet sink
using PyFlink's Table API, offers no benefit for a 15-minute SLA.

**Winner: Spark** — the 15-minute window SLA is perfectly suited to Spark's
micro-batch trigger model.

---

## Dimension 3: Recovery Time Objective (RTO)

### Bus Bunching Detection — RTO: < 5 minutes

The bus bunching detector tracks per-pair "close since" timestamps. If the
Flink job fails and recovers after 5 minutes without checkpointing, previously
tracked close pairs would be lost — potentially missing a bunching event that
straddles the failure window.

**Flink** solves this with checkpointing: the three MapStates
(`bus_positions`, `close_pairs`, `alerted_pairs`) are checkpointed to the
`flink-checkpoints` Docker volume every 10 seconds. On restart, Flink replays
Kafka events from the last checkpoint offset, restoring the exact MapState
before the failure. The RTO is < 30 seconds (checkpoint replay + job restart).

**Spark** does not provide equivalent per-group state recovery for
`flatMapGroupsWithState` — recovering from a failure requires replaying all
events from the Kafka beginning-offset (or earliest checkpoint), which in a
production environment could take tens of minutes for a long-retention topic.

**Winner: Flink** — native keyed state checkpointing provides the < 5-minute
RTO required for the transit operations SLA.

---

### Ward Energy Aggregation — RTO: < 30 minutes

The ward energy job is stateless within each micro-batch (Spark recomputes
aggregations from buffered Kafka events). Spark checkpoints the streaming
query state (watermark position and active window buffers) to
`/tmp/spark-checkpoints/ward_energy_*`. On restart, Spark resumes from the
last checkpoint and replays any buffered events. RTO is typically < 5 minutes.

Since the 45-minute watermark means a ward's window does not close until
45 minutes after the event time, even a 30-minute recovery does not cause
data loss — late events are still within the watermark window.

**Winner: Spark** — the 45-minute watermark provides a natural recovery buffer
that makes Spark's simpler checkpoint model sufficient for this use case.

---

## Dimension 4: Operational Complexity

### Bus Bunching Detection — Flink Preferred

Deploying a Flink DataStream job with keyed MapState requires:
- A Flink cluster (JobManager + TaskManager)
- State backend configuration (filesystem for this assignment, RocksDB for production)
- Kafka connector JARs in the Flink lib directory

However, once deployed, the **Flink programming model for stateful stream
processing is cleaner than the equivalent Spark approach**. The
`KeyedProcessFunction` directly expresses the per-entity state machine logic.
The equivalent Spark `flatMapGroupsWithState` requires managing a custom
state update function with arbitrary state objects — less intuitive and harder
to reason about.

For ongoing operations: Flink's UI (port 8081) provides per-operator
throughput, checkpoint status, and keyed state size metrics that allow
operators to observe the bunching detection state in real time.

**Verdict**: The initial deployment complexity of Flink is higher, but the
ongoing operational visibility and the expressiveness of its stateful
processing model make it the better long-term choice for incident detection.

---

### Ward Energy Aggregation — Spark Preferred

The Spark Structured Streaming ward energy job uses:
- A standard `groupBy(window(...), ward_id).agg(...)` — directly supported
  by Spark SQL with no custom state management
- The dual Kafka + Parquet sink is expressed with two `writeStream` calls,
  each with its own checkpoint location — simple and well-documented
- The `spark-sql-kafka` connector is loaded via `--packages` at submit time;
  no pre-installation of JARs is required

For ongoing operations: the Spark UI (port 8080) shows streaming query
progress, batch durations, and input/output rates. The ward energy query's
simple structure makes performance debugging straightforward.

**Verdict**: Spark's SQL-first design reduces both implementation and
maintenance complexity for windowed aggregations. The same logic that runs in
the streaming job can be tested interactively with `spark.read` on the Parquet
output — a significant advantage for councillor-facing analytics.

---

# New Kafka Topics

| Topic | Partitions | Retention | Purpose |
|---|---|---|---|
| `urbanpulse.incidents` | 3 | 7 Days | Flink alert output (all 3 detectors) |
| `urbanpulse.ward_energy_summary` | 3 | 30 Days | Spark ward energy aggregation output |
| `urbanpulse.health_advisories` | 3 | 7 Days | Spark AQI health advisory output |

---

# Repository Structure

```text
taskC/
│
├── Dockerfile.flink              Custom Flink 1.18 image with PyFlink + Kafka JARs
├── docker-compose.yml            Flink cluster (JobManager + TaskManager) + Spark
├── requirements.txt              Python deps for local scripts
├── start.sh                      Full demo orchestration (start everything)
├── stop.sh                       Stop all Task C services
│
├── smart_meter_producer.py       Smart meter Kafka producer (Task B gap filled here)
│
├── flink/
│   ├── aqi_emergency_detector.py (C1a) AQI > 300 → immediate alert
│   ├── gridlock_detector.py      (C1b) 3 consecutive junction wait > 180s
│   └── bunching_detector.py      (C1c) 2 buses < 200m for 5+ minutes
│
├── spark/
│   ├── ward_energy_streaming.py  (C2)  15-min tumbling window, dual Kafka+Parquet sink
│   └── health_advisories.py      (C3)  10-min rolling AQI SQL + zone join + filter
│
├── data/
│   └── zone_profile.csv          Static zone reference table (5 zones)
│
├── kafka/
│   └── create_taskc_topics.sh    Create 3 new Kafka output topics
│
├── run_flink_jobs.sh             Submit all 3 Flink jobs
├── run_spark_jobs.sh             Submit both Spark jobs
│
└── README.md                     This file
```

---

# How to Run

## Prerequisites

1. Docker Desktop running
2. Task B Kafka cluster running at `localhost:9092`
3. Task B producers running: `air_quality_producer.py`, `traffic_signal_producer.py`,
   `bus_gps_producer.py`
4. Python 3.9+ with `kafka-python` installed (`pip install kafka-python==3.0.9`)

## Quick Start

```bash
# 1. Navigate to taskC
cd taskC/

# 2. Start everything with one command
chmod +x start.sh && ./start.sh
```

## Manual Steps

```bash
# 1. Build and start Docker services
docker compose up -d --build

# 2. Create Kafka topics (using Task B's kafka-topics.sh or a Kafka container)
bash kafka/create_taskc_topics.sh

# 3. Start smart meter producer
python3 smart_meter_producer.py &

# 4. Submit Flink jobs (after JobManager is healthy at http://localhost:8081)
bash run_flink_jobs.sh

# 5. Submit Spark jobs (after Spark is ready at http://localhost:8080)
bash run_spark_jobs.sh
```

## Verify Output

```bash
# Verify Flink incidents (all 3 alert types)
kafka-console-consumer.sh --topic urbanpulse.incidents \
    --bootstrap-server localhost:9092 --from-beginning

# Verify Spark ward energy summaries
kafka-console-consumer.sh --topic urbanpulse.ward_energy_summary \
    --bootstrap-server localhost:9092 --from-beginning

# Verify Spark health advisories
kafka-console-consumer.sh --topic urbanpulse.health_advisories \
    --bootstrap-server localhost:9092 --from-beginning

# Verify Parquet output (requires pyarrow: pip install pyarrow)
python3 -c "
import pandas as pd
df = pd.read_parquet('output/ward_energy/', engine='pyarrow')
print(df.head(20))
print('Partitions:', df['ward_id'].unique())
"
```

---

# Status

## Completed

- Docker Compose for Flink cluster and Spark
- Smart meter producer (fills Task B gap)
- Flink AQI Emergency Detector (keyed state + event-time watermarks)
- Flink Traffic Gridlock Detector (ListState, 3-consecutive-cycle logic)
- Flink Bus Bunching Detector (MapState, Haversine distance, 5-minute duration tracking)
- Spark Ward Energy Streaming (15-min tumbling window, 45-min watermark, dual sink)
- Spark Health Advisories SQL (10-min rolling avg, zone_profile join, Update mode)
- Flink vs Spark comparison (4 dimensions, 2 use cases)
- Kafka topic creation script for Task C output topics

## Relationship to Other Tasks

- Task A's Kappa architecture is realised here: the Spark job's Parquet sink
  (`/output/ward_energy/`) is the "historical replay / government reporting"
  layer described in the architecture document, without a separate batch system.
- Task B's Kafka topics (`urbanpulse.air_quality`, `urbanpulse.traffic_signals`,
  `urbanpulse.bus_gps`, `urbanpulse.smart_meters`) are the sources for all Task C jobs.
