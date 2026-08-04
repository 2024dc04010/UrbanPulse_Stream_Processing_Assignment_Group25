# Task B - Apache Kafka Multi-Source Urban Data Ingestion

## UrbanPulse - Real-Time Urban Operations Intelligence Platform

### Course
Stream Processing and Analytics

### Domain
Smart Cities & Urban Infrastructure

---

# Overview

Task B implements the Apache Kafka ingestion and processing layer for the UrbanPulse platform.

The solution ingests and processes multiple independent urban data streams:

- Bus GPS telemetry
- Traffic signal events
- Air quality sensor readings
- Smart meter energy consumption data

The implementation includes:

- Kafka topic design and retention policies
- Python producers
- Priority consumer architecture
- Kafka Streams route enrichment
- Dead Letter Queue (DLQ) processing and reporting

---

# Kafka Topics

| Topic | Purpose |
|---------|---------|
| urbanpulse.bus_gps | Real-time bus GPS telemetry |
| urbanpulse.air_quality | Air quality sensor events |
| urbanpulse.traffic_signals | Traffic signal status updates |
| urbanpulse.smart_meters | Smart meter readings |
| urbanpulse.route_schedule | Route metadata KTable source |
| urbanpulse.enriched_bus_gps | Enriched GPS stream |
| urbanpulse.dlq | Dead Letter Queue |

---

# Partition Strategy

| Topic | Partitions | Justification |
|---------|---------|---------|
| urbanpulse.bus_gps | 12 | Highest-volume stream (~2400 events/sec). Supports parallel processing while preserving route-level ordering using route_id as the message key. |
| urbanpulse.traffic_signals | 6 | Supports both real-time control and dashboard analytics workloads. |
| urbanpulse.air_quality | 3 | Lower-volume sensor stream (~60 events/sec). |
| urbanpulse.smart_meters | 8 | Supports future Spark-based ward-level aggregation workload. |
| urbanpulse.route_schedule | 3 | Kafka Streams KTable source topic. |
| urbanpulse.enriched_bus_gps | 3 | Output of route enrichment application. |
| urbanpulse.dlq | 1 | Low-volume validation failure topic. |

---

# Topic Retention Policies

| Topic | Retention | Justification |
|---------|---------|---------|
| urbanpulse.bus_gps | 24 Hours | Accident investigation and route replay. |
| urbanpulse.air_quality | 90 Days | Pollution trend analysis and environmental review. |
| urbanpulse.smart_meters | 365 Days | Regulatory energy audits and annual consumption analysis. |
| urbanpulse.traffic_signals | 7 Days | Congestion replay and operational diagnostics. |
| urbanpulse.dlq | 30 Days | Validation failure investigation and debugging. |

---

# Producer Implementations

## bus_gps_producer.py

Purpose:

Publishes real-time bus telemetry into:

```text
urbanpulse.bus_gps
```

Features:

- Uses `route_id` as Kafka message key
- Guarantees ordering per route
- Generates realistic GPS coordinates
- Produces JSON-formatted Kafka events

Example Event:

```json
{
  "bus_id": "BUS104",
  "route_id": "R12",
  "lat": 12.9784,
  "lon": 77.6408,
  "speed_kmh": 42,
  "timestamp": "2026-08-01T12:30:42Z"
}
```

---

## air_quality_producer.py

Purpose:

Publishes air quality sensor readings into:

```text
urbanpulse.air_quality
```

### Reliability Features

The producer implements at-least-once delivery semantics using:

- `acks='all'`
- `retries=5`
- `max_in_flight_requests_per_connection=1`
- Explicit retry loop with backoff
- Kafka delivery acknowledgement using `future.get(timeout=10)`
- Kafka exception handling

### Retry Behaviour

The producer performs up to three explicit send attempts.

On failure:

- Error is logged
- Retry delay is applied
- Event is retransmitted
- Failure is recorded if all attempts fail

### Simulated Fault Conditions

#### 5% NULL AQI Events

Approximately 5% of events contain:

```text
aqi = None
```

These events are logged and routed to the DLQ during validation.

#### Out-of-Range AQI Events

The producer periodically generates values such as:

```text
-10
999
```

for DLQ testing.

#### Sensor Timeout Simulation

The producer periodically simulates sensor timeout events using:

```text
TimeoutError
```

which triggers retry logic.

### Logged Events

Example log records:

```text
Null AQI generated and handled gracefully.
```

```text
Out-of-range AQI generated for DLQ testing.
```

```text
Send attempt 1 failed for air_quality event.
Retrying air_quality event after short backoff.
```

```text
Sent air_quality event successfully.
```

Primary Topic:

```text
urbanpulse.air_quality
```

---

## traffic_signal_producer.py

Features:

- Publishes traffic signal cycle events
- Produces junction-level congestion metrics
- Keyed using `junction_id`
- Used by priority consumer architecture

Primary Topic:

```text
urbanpulse.traffic_signals
```

---

# Priority Consumer Architecture

Two independent Kafka consumer groups process the traffic signal stream.

## HIGH_PRIORITY Group

Consumer Group:

```text
signal_control_group
```

Purpose:

- Real-time signal control system
- No artificial processing delay
- Maintains near-zero lag

Consumer:

```text
high_priority_consumer.py
```

---

## STANDARD_PRIORITY Group

Consumer Group:

```text
analytics_dashboard_group
```

Purpose:

- Dashboard analytics workload
- Simulated slow processing
- Demonstrates lag growth under load

Consumer:

```text
standard_priority_consumer.py
```

Implementation deploys multiple consumer instances to support analytical processing.

---

# Kafka Streams Route Enrichment

Task B Q7 is implemented using Kafka Streams.

## KStream Source

```text
urbanpulse.bus_gps
```

## KTable Source

```text
urbanpulse.route_schedule
```

Configuration:

```text
cleanup.policy=compact
```

The route schedule topic acts as a Kafka Streams KTable containing the latest route metadata per route.

---

## Kafka Streams Application

Implementation:

```text
taskB/kafka_streams/route_enrichment/RouteEnrichmentApp.java
```

The application performs a KStream-KTable left join using:

```text
route_id
```

as the join key.

---

## Enriched Output

Output Topic:

```text
urbanpulse.enriched_bus_gps
```

Additional fields added:

```text
route_name
terminal
scheduled_arrival_time
```

This enriched stream forms the foundation for future ETA and transit analytics services.

---

# Python Enrichment Prototype

For comparison and development purposes, an earlier prototype implementation is also preserved:

```text
taskB/python_enrichment_prototype/
```

This version performs enrichment using:

```text
route_schedule.csv
```

and an in-memory lookup table.

The Kafka Streams implementation represents the final Task B solution.

---

# Dead Letter Queue (DLQ)

Topic:

```text
urbanpulse.dlq
```

Implementation:

```text
taskB/dlq/dlq_processor.py
```

Validation Rules:

- NULL_AQI
- AQI_OUT_OF_RANGE
- NULL_GPS_COORDINATE
- INVALID_GPS_COORDINATES

DLQ messages include:

- error_reason
- source_topic
- partition
- offset
- original_event

---

# DLQ Reporting

Implementation:

```text
taskB/dlq/dlq_report.py
```

Features:

- 5-minute reporting window
- Error type aggregation
- DLQ distribution reporting

Example error categories:

```text
NULL_AQI
AQI_OUT_OF_RANGE
INVALID_GPS_COORDINATES
```

---

# Repository Structure

```text
taskB/
│
├── consumers/
│   ├── high_priority_consumer.py
│   └── standard_priority_consumer.py
│
├── dlq/
│   ├── dlq_processor.py
│   └── dlq_report.py
│
│
├── kafka/
│   ├── create_topics.sh
│   ├── set_retention.sh
│   └── topic_design.md
│
├── kafka_streams/
│   └── route_enrichment/
│       ├── RouteEnrichmentApp.java
│       ├── publish_route_schedule.py
│       └── route_schedule.csv
│
├── producers/
│   ├── bus_gps_producer.py
│   ├── air_quality_producer.py
│   └── traffic_signal_producer.py
│
└── python_enrichment_prototype/
    ├── route_enrichment.py
    └── route_schedule.csv
```

---

# Status

## Completed

- Kafka topic creation
- Topic partitioning strategy
- Topic retention policies
- Bus GPS producer
- Air Quality producer
- Traffic Signal producer
- Priority consumer architecture
- Kafka Streams route enrichment
- Dead Letter Queue processing
- DLQ reporting
- GitHub repository backup

## Next Phase

Task C:

- Apache Flink Incident Detection
- Spark Structured Streaming Analytics
- Streaming SQL Health Advisories
- Flink vs Spark Comparison
