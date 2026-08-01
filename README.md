# UrbanPulse - Real-Time Urban Operations Intelligence Platform

## Domain

Smart Cities & Urban Infrastructure

## Assignment Scope

UrbanPulse is a real-time urban operations intelligence platform designed to ingest, process, validate and analyse multiple city data streams including:


- Public transport GPS telemetry

- Traffic signal events

- Air quality sensor readings

- Smart meter energy consumption data


The platform uses Apache Kafka as the event ingestion layer and follows an event-driven architecture suitable for real-time analytics.


---


# Task B - Apache Kafka Multi-Source Urban Data Ingestion


## Kafka Topics


| Topic | Purpose |

|---------|---------|

| urbanpulse.bus_gps | Real-time bus GPS telemetry |

| urbanpulse.air_quality | Air quality sensor events |

| urbanpulse.traffic_signals | Smart traffic signal status updates |

| urbanpulse.smart_meters | Energy meter readings |

| urbanpulse.dlq | Dead Letter Queue for validation failures |


---


## Partition Strategy


### urbanpulse.bus_gps - 12 Partitions


Highest throughput stream (~2400 events/sec). Partitioning supports downstream parallelism while maintaining route-level ordering using `route_id` as the Kafka message key.


### urbanpulse.traffic_signals - 6 Partitions


Moderate volume stream (~380 events/sec). Supports simultaneous operational and analytical consumer workloads.


### urbanpulse.air_quality - 3 Partitions


Lower-volume stream (~60 events/sec). Provides sufficient throughput while minimizing partition overhead.


### urbanpulse.smart_meters - 8 Partitions


High-volume stream (~1100 events/sec). Supports scalable ward-level energy analytics and future Spark processing.


### urbanpulse.dlq - 1 Partition


Low-volume validation error stream used for operational troubleshooting and reporting.


---


## Retention Policy


| Topic | Retention | Justification |

|---------|---------|---------|

| urbanpulse.bus_gps | 24 Hours | Accident investigations and route replay |

| urbanpulse.air_quality | 90 Days | AQI trend analysis and environmental review |

| urbanpulse.smart_meters | 365 Days | Regulatory energy audit requirements |

| urbanpulse.traffic_signals | 7 Days | Traffic replay and congestion analysis |

| urbanpulse.dlq | 30 Days | Validation failure analysis and debugging |


---


## Implemented Producers


### bus_gps_producer.py


Features:


- Uses `route_id` as Kafka message key

- Preserves ordering of GPS events per route

- Generates realistic bus telemetry events

- Produces JSON-formatted Kafka messages


---


### air_quality_producer.py


Features:


- At-least-once delivery configuration

- `acks='all'`

- `retries=5`

- Explicit retry handling

- Simulated sensor timeout events

- 5% NULL AQI event generation

- Out-of-range AQI event generation for DLQ testing

- Detailed producer logging


---


## Dead Letter Queue Implementation


### dlq_processor.py


Validation rules:


- NULL_AQI

- AQI_OUT_OF_RANGE

- NULL_GPS_COORDINATE

- INVALID_GPS_COORDINATES


Invalid records are enriched with:


- error_reason

- source_topic

- partition

- offset

- original_event


and routed to:


```text

urbanpulse.dlq
