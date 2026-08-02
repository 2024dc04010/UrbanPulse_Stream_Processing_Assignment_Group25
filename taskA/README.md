# Task A - Architecture Design: Lambda vs Kappa for a Dual-Reporting City Platform

## UrbanPulse - Real-Time Urban Operations Intelligence Platform

Stream Processing and Analytics
Domain: Smart Cities & Urban Infrastructure | 20 Marks

---

# Overview

Task A covers the architecture design decision for UrbanPulse: a labelled
system diagram, a Lambda vs Kappa evaluation matrix scoped to UrbanPulse's
dual real-time/government-reporting requirement, and a government
deployment readiness checklist.

Full submission document: `UrbanPulse_TaskA_Report.pdf`

---

# Contents

| Problem Statement | Deliverable | Marks |
|---|---|---|
| 1. Architecture diagram | All four data sources, Kafka ingestion, Flink + Spark Structured Streaming processing lanes, storage layer, serving layer | 6 |
| 2. Lambda vs Kappa matrix | Latency, Fault Tolerance, Operational Complexity, Reprocessing Capability, Cost, Compliance with Government Reporting Mandate | 7 |
| 3. Architecture readiness checklist | Data sovereignty, open-source mandate, disaster recovery (RPO < 15 min, RTO < 30 min), ward officer accessibility | 7 |

---

# Architecture Choice: Kappa

UrbanPulse adopts a Kappa architecture: a single stream-processing lane
(Kafka + Flink + Spark Structured Streaming) serves both the sub-2-minute
AQI/signal-control SLAs and the weekly/monthly government reporting
mandate, rather than maintaining a separate batch layer. The deciding
factor is that Task B's long-retention Kafka topics (90-day air quality,
365-day smart meters) and Task C's Spark Structured Streaming job (which
writes a ward_id + date-partitioned Parquet sink alongside its Kafka
output) already provide the reprocessing and reporting capability a
batch layer would normally exist to deliver — so a second codebase would
only duplicate logic and create a second source of truth.

Full reasoning, the complete evaluation matrix, and the readiness
checklist are in the PDF report.

---

# Storage Technology Choices

| Data Type | Technology | Reason |
|---|---|---|
| Time-series sensor data (raw AQI + smart-meter readings) | TimescaleDB / InfluxDB | Native retention/rollup policies matching Task B's 90-day and 365-day requirements |
| Geospatial bus positions | PostgreSQL + PostGIS, with a Redis geo index | PostGIS for durable trajectory history; Redis for sub-millisecond "latest position" lookups |
| Historical AQI records | Parquet on MinIO/HDFS, partitioned by ward_id and date | Produced by the Task C Spark job's dual sink; queried via Trino/Presto for audit |
| Councillor report aggregates | Apache Druid / ClickHouse | OLAP roll-ups backing the councillor dashboard and scheduled report exports |

---

# Assumptions, Scope & Trade-offs

- UrbanPulse is deployed on government-owned or sovereign infrastructure.
- Kafka topics are configured with retention sufficient to support the
  periods defined in Task B.
- Simulated data rates used in the Task B/C prototypes are lower than the
  production-scale rates described in this document.
- Councillor reports are generated from retained streaming outputs
  (Kafka + the Parquet lake) rather than a separate batch-processing
  system — this is the practical expression of the Kappa choice above.
- Smart-meter retention: keeping the full 365-day regulatory window
  live in Kafka would need roughly 21 TB of broker disk at replication
  factor 3 (1,100 events/sec x 365 days x RF3). The design instead treats
  Kafka as operational storage and relies on the Parquet/MinIO lake,
  already a sink of the Task C Spark job, for cost-efficient long-term
  archival.

See the PDF report's "Assumptions, Scope & Trade-offs" section for the
full list, including production-vs-prototype scope notes and planned
future enhancements.

---

# Files

```text
taskA/
└── README.md                 (this file)

UrbanPulse_TaskA_Report.pdf   Full Task A submission (diagram, matrix,
                              checklist, assumptions & trade-offs)
UrbanPulse_TaskA_Report.docx  Editable source for the above
```

---

# Status

Completed. See `taskB/README.md` for the Kafka implementation this
architecture decision informs, and `taskC/README.md` for the Flink/Spark
processing layer.
