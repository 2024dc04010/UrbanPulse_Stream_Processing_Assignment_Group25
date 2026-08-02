# Task B - Apache Kafka Multi-Source Urban Data Ingestion



## UrbanPulse - Real-Time Urban Operations Intelligence Platform




### Domain
Smart Cities & Urban Infrastructure



---



# Overview



Task B implements the Apache Kafka ingestion and processing layer for the UrbanPulse platform.



The solution ingests and processes multiple independent urban data streams:



- Bus GPS telemetry
- Traffic signal events
- Air quality sensor readings
- Smart meter energy readings



The implementation includes:



- Kafka topic design and retention policies
- Python data producers
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
| urbanpulse.smart_meters | Smart meter energy readings |
| urbanpulse.route_schedule | Route metadata KTable source |
| urbanpulse.enriched_bus_gps | Enriched GPS stream |
| urbanpulse.dlq | Dead Letter Queue |
| urbanpulse.incidents | Reserved for Task C incident alerts |



---



# Partition Strategy



| Topic | Partitions | Justification |
|---------|---------|---------|
| urbanpulse.bus_gps | 12 | Highest-volume stream (~2400 events/sec). Supports downstream parallelism while preserving route-level ordering through route_id keying. |
| urbanpulse.traffic_signals | 6 | Moderate-volume stream consumed by both real-time control and analytical workloads. |
| urbanpulse.air_quality | 3 | Lower-volume stream (~60 events/sec). Provides adequate throughput and fault isolation. |
| urbanpulse.smart_meters | 8 | Supports ward-level aggregation and future Spark analytics workloads. |
| urbanpulse.route_schedule | 3 | Supports Kafka Streams KTable distribution. |
| urbanpulse.enriched_bus_gps | 3 | Supports enriched output processing. |
| urbanpulse.dlq | 1 | Low-volume operational topic used for validation failures. |



---



# Topic Retention Policies



| Topic | Retention | Justification |
|---------|---------|---------|
| urbanpulse.bus_gps | 24 Hours | Supports accident investigation and route replay. |
| urbanpulse.air_quality | 90 Days | Supports pollution trend analysis and environmental review. |
| urbanpulse.smart_meters | 365 Days | Supports regulatory energy audits and historical consumption analysis. |
| urbanpulse.traffic_signals | 7 Days | Supports congestion replay and operational diagnostics. |
| urbanpulse.dlq | 30 Days | Supports debugging and validation failure analysis. |



---



# Producer Implementations



## bus_gps_producer.py



Features:



- Publishes real-time bus telemetry
- Uses `route_id` as Kafka message key
- Preserves ordering of events per route
- Emits realistic GPS coordinates and route activity



Primary Topic:



```text
urbanpulse.bus_gps
