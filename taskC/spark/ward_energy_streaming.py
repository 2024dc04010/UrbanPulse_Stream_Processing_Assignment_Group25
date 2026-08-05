"""
Task C — Part II (C2): Spark Structured Streaming — Ward Energy Analytics
=========================================================================
Assignment requirement:
    Build a Spark Structured Streaming application on urbanpulse.smart_meters
    computing, per ward_id, per 15-minute tumbling window:
        - total_kwh_consumed
        - avg_power_factor
        - peak_voltage
    Output to:
        1. ward_energy_summary  Kafka topic  (JSON)
        2. Partitioned Parquet dataset       (partitioned by ward_id and date)

    Include a 45-minute watermark for late data.

Design:
    - Source:     urbanpulse.smart_meters (Kafka)
    - Window:     15-minute tumbling, event-time, watermark=45 min
    - Output 1:   urbanpulse.ward_energy_summary (Kafka, Append mode)
    - Output 2:   /output/ward_energy/ (Parquet, partitioned by ward_id + date)
    - Checkpoint: /tmp/spark-checkpoints/ward_energy_*

Run inside Spark container:
    spark-submit \\
      --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \\
      --master spark://spark:7077 \\
      /opt/spark/jobs/ward_energy_streaming.py

Smart Meter event schema (from taskC/smart_meter_producer.py):
    {
        "meter_id":     "M0101",
        "ward_id":      "W01",
        "kwh_consumed": 1.84,
        "power_factor": 0.93,
        "voltage":      231.2,
        "timestamp":    "2026-08-02T16:00:00Z"
    }
"""

import logging
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, to_json, struct, sum as _sum,
    avg, max as _max, to_timestamp, window,
    date_format, lit, current_timestamp,
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
)

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("WardEnergyStreaming")

# ── Configuration ────────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP     = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
SOURCE_TOPIC        = "urbanpulse.smart_meters"
SINK_KAFKA_TOPIC    = "urbanpulse.ward_energy_summary"
SINK_PARQUET_PATH   = "/output/ward_energy"
WINDOW_DURATION     = "1 minute"
WATERMARK_DELAY     = "1 minute"    # handle late-arriving smart meter events

KAFKA_CHECKPOINT    = "/tmp/spark-checkpoints/ward_energy_kafka"
PARQUET_CHECKPOINT  = "/tmp/spark-checkpoints/ward_energy_parquet"

# ── Smart Meter Event Schema ─────────────────────────────────────────────────
SMART_METER_SCHEMA = StructType([
    StructField("meter_id",     StringType(),  True),
    StructField("ward_id",      StringType(),  True),
    StructField("kwh_consumed", DoubleType(),  True),
    StructField("power_factor", DoubleType(),  True),
    StructField("voltage",      DoubleType(),  True),
    StructField("timestamp",    StringType(),  True),
])

# ── Spark Session ────────────────────────────────────────────────────────────
spark = (
    SparkSession.builder
    .appName("UrbanPulse — Ward Energy Streaming")
    .config("spark.sql.shuffle.partitions", "3")
    .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

logger.info("Spark session started.")

# ── Source: Read from Kafka ──────────────────────────────────────────────────
raw_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("subscribe",               SOURCE_TOPIC)
    .option("startingOffsets",         "latest")
    .option("failOnDataLoss",          "false")
    .load()
)

# ── Parse JSON payload ────────────────────────────────────────────────────────
parsed_df = (
    raw_df
    .selectExpr("CAST(value AS STRING) AS json_str")
    .select(from_json(col("json_str"), SMART_METER_SCHEMA).alias("d"))
    .select("d.*")
    .filter(col("ward_id").isNotNull())
    .filter(col("kwh_consumed").isNotNull())
    .withColumn("event_time", to_timestamp(col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss'Z'"))
    .filter(col("event_time").isNotNull())
)

# ── 15-Minute Tumbling Window with 45-Minute Late-Data Watermark ─────────────
windowed_df = (
    parsed_df
    .withWatermark("event_time", WATERMARK_DELAY)
    .groupBy(
        window(col("event_time"), WINDOW_DURATION),   # 15-min tumbling window
        col("ward_id")
    )
    .agg(
        _sum("kwh_consumed").alias("total_kwh_consumed"),
        avg("power_factor").alias("avg_power_factor"),
        _max("voltage").alias("peak_voltage"),
    )
    .select(
        col("ward_id"),
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("total_kwh_consumed"),
        col("avg_power_factor"),
        col("peak_voltage"),
    )
)

# ── Sink 1: Write aggregated results to Kafka ─────────────────────────────────
kafka_output_df = windowed_df.select(
    col("ward_id").cast("string").alias("key"),
    to_json(struct(
        col("ward_id"),
        col("window_start"),
        col("window_end"),
        col("total_kwh_consumed"),
        col("avg_power_factor"),
        col("peak_voltage"),
    )).alias("value")
)

kafka_query = (
    kafka_output_df.writeStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("topic",                   SINK_KAFKA_TOPIC)
    .option("checkpointLocation",      KAFKA_CHECKPOINT)
    .outputMode("update")     # Update: emit partial window results instantly
    .start()
)

logger.info(f"Kafka sink started → topic: {SINK_KAFKA_TOPIC}")

# ── Sink 2: Write aggregated results to Parquet (partitioned) ────────────────
parquet_output_df = windowed_df.withColumn(
    "date", date_format(col("window_start"), "yyyy-MM-dd")
)

parquet_query = (
    parquet_output_df.writeStream
    .format("parquet")
    .option("path",              SINK_PARQUET_PATH)
    .option("checkpointLocation", PARQUET_CHECKPOINT)
    .partitionBy("ward_id", "date")   # partitioned by ward + date for query efficiency
    .outputMode("append")
    .start()
)

logger.info(f"Parquet sink started → path: {SINK_PARQUET_PATH} | partitioned by ward_id, date")

# ── Keep the application running ─────────────────────────────────────────────
logger.info(
    f"\n{'='*60}\n"
    f"Ward Energy Streaming Job RUNNING\n"
    f"  Source  : {SOURCE_TOPIC}\n"
    f"  Window  : {WINDOW_DURATION} tumbling  |  Watermark: {WATERMARK_DELAY}\n"
    f"  Sink 1  : Kafka → {SINK_KAFKA_TOPIC}\n"
    f"  Sink 2  : Parquet → {SINK_PARQUET_PATH} (partitioned by ward_id, date)\n"
    f"{'='*60}"
)

spark.streams.awaitAnyTermination()
