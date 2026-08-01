# Kafka Topic Design and Retention Policy


## Topic Partitioning


- `urbanpulse.bus_gps`: 12 partitions  

  Highest-volume stream at approximately 2,400 events/sec. Twelve partitions provide downstream parallelism while preserving route-level ordering using `route_id` as the Kafka key.


- `urbanpulse.smart_meters`: 8 partitions  

  High-volume stream at approximately 1,100 events/sec. Eight partitions support scalable ward-level energy aggregation and later Spark processing.


- `urbanpulse.traffic_signals`: 6 partitions  

  Moderate-volume operational stream at approximately 380 events/sec. Six partitions support both high-priority signal control consumers and standard dashboard consumers.


- `urbanpulse.air_quality`: 3 partitions  

  Lower-volume stream at approximately 60 events/sec. Three partitions are sufficient while still providing fault isolation and parallel consumption.


- `urbanpulse.dlq`: 1 partition  

  Low-volume error topic. One partition is sufficient for ordered DLQ inspection and reporting.


## Retention Policy


- `urbanpulse.bus_gps`: 24 hours  

  Supports accident investigation and short-term route replay.


- `urbanpulse.air_quality`: 90 days  

  Supports pollution trend analysis and seasonal AQI review.


- `urbanpulse.smart_meters`: 365 days  

  Supports regulatory energy audits and historical ward-level consumption analysis.


- `urbanpulse.traffic_signals`: 7 days  

  Supports operational replay and short-term congestion analysis.


- `urbanpulse.dlq`: 30 days  

  Supports validation failure analysis and debugging without keeping failed records indefinitely.


## Prototype Note


The production design assumes a 3-broker Kafka cluster with replication factor 3. The academic prototype runs on a single-broker Rocky Linux VM with replication factor 1 due to resource constraints, while preserving the same logical topic, partition, and retention design.
