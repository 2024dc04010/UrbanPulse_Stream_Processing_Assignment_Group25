"""
Task C — Part II (C3): Spark Streaming SQL — AQI Health Advisories
===================================================================
Assignment requirement:
    Write a Streaming SQL query on the AQI stream that:
        (a) computes a 10-minute rolling average AQI per zone
        (b) joins with a static zone_profile table (zone name, population,
            number of schools) to produce an enriched advisory
        (c) filters for rolling_avg_aqi > 150 (Unhealthy)
        (d) writes to urbanpulse.health_advisories topic
    Use Update output mode.

Design:
    - Source:    urbanpulse.air_quality (Kafka)
    - Window:    10-minute SLIDING window (every 1 min) per zone for rolling avg
    - Join:      Static zone_profile DataFrame (loaded from CSV)
    - Filter:    rolling_avg_aqi > 150
    - Output:    urbanpulse.health_advisories (Kafka, Update mode)
    - Checkpoint: /tmp/spark-checkpoints/health_advisories

Run inside Spark container:
    spark-submit \\
      --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \\
      --master spark://spark:7077 \\
      /opt/spark/jobs/health_advisories.py
"""

import logging
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, to_json, struct,
    avg, to_timestamp, window,
    lit, when,
)
from pyspark.sql.types import (
    StructType, StructField, StringType,
    DoubleType, IntegerType,
)

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("HealthAdvisories")

# ── Configuration ────────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP      = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
SOURCE_TOPIC         = "urbanpulse.air_quality"
SINK_TOPIC           = "urbanpulse.health_advisories"
ZONE_PROFILE_PATH    = "taskC/data/zone_profile.csv"
WINDOW_DURATION      = "10 minutes"    # rolling window width
SLIDE_DURATION       = "1 minute"      # slide interval (produces rolling effect)
WATERMARK_DELAY      = "10 minutes"    # tolerate up to 10 min late events
AQI_ALERT_THRESHOLD  = 150             # Unhealthy threshold (US EPA standard)
CHECKPOINT_PATH      = "/tmp/spark-checkpoints/health_advisories"

# ── Air Quality Event Schema ─────────────────────────────────────────────────
AQI_SCHEMA = StructType([
    StructField("sensor_id",  StringType(),  True),
    StructField("zone",       StringType(),  True),
    StructField("aqi",        DoubleType(),  True),
    StructField("pm25",       DoubleType(),  True),
    StructField("pm10",       DoubleType(),  True),
    StructField("no2",        DoubleType(),  True),
    StructField("timestamp",  StringType(),  True),
])

# ── Zone Profile Schema (static CSV) ─────────────────────────────────────────
ZONE_PROFILE_SCHEMA = StructType([
    StructField("zone",          StringType(),  True),
    StructField("zone_name",     StringType(),  True),
    StructField("population",    IntegerType(), True),
    StructField("num_schools",   IntegerType(), True),
    StructField("num_hospitals", IntegerType(), True),
    StructField("area_sq_km",    IntegerType(), True),
])

# ── Spark Session ────────────────────────────────────────────────────────────
spark = (
    SparkSession.builder
    .appName("UrbanPulse — AQI Health Advisories")
    .config("spark.sql.shuffle.partitions", "3")
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

logger.info("Spark session started.")

# ── Load Static Zone Profile ──────────────────────────────────────────────────
zone_profile_df = (
    spark.read
    .format("csv")
    .schema(ZONE_PROFILE_SCHEMA)
    .option("header", "true")
    .load(ZONE_PROFILE_PATH)
)

zone_profile_df.cache()
logger.info(f"Loaded zone_profile with {zone_profile_df.count()} zones.")
zone_profile_df.show()

# ── Register as SQL views (Streaming SQL approach) ───────────────────────────
zone_profile_df.createOrReplaceTempView("zone_profile")

# ── Source: Read AQI stream from Kafka ───────────────────────────────────────
raw_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("subscribe",               SOURCE_TOPIC)
    .option("startingOffsets",         "latest")
    .option("failOnDataLoss",          "false")
    .load()
)

# ── Parse JSON and extract valid AQI readings ────────────────────────────────
parsed_df = (
    raw_df
    .selectExpr("CAST(value AS STRING) AS json_str")
    .select(from_json(col("json_str"), AQI_SCHEMA).alias("d"))
    .select("d.*")
    .filter(col("zone").isNotNull())
    .filter(col("aqi").isNotNull())
    .filter((col("aqi") >= 0) & (col("aqi") <= 500))   # valid AQI range
    .withColumn("event_time", to_timestamp(col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss'Z'"))
    .filter(col("event_time").isNotNull())
)

# Register the parsed stream as a temp view for Streaming SQL
parsed_df.createOrReplaceTempView("aqi_stream")

# ── (a) 10-Minute Rolling Average AQI per Zone (Streaming SQL) ───────────────
#
# Spark Structured Streaming supports SQL windowed aggregations on streams.
# We use a 10-minute sliding window with 1-minute slide to get a rolling average.
# Update output mode emits a result for every trigger that has new/updated data.
#
rolling_aqi_df = (
    parsed_df
    .withWatermark("event_time", WATERMARK_DELAY)
    .groupBy(
        window(col("event_time"), WINDOW_DURATION, SLIDE_DURATION),
        col("zone")
    )
    .agg(
        avg("aqi").alias("rolling_avg_aqi"),
        avg("pm25").alias("rolling_avg_pm25"),
        avg("pm10").alias("rolling_avg_pm10"),
    )
    .select(
        col("zone"),
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("rolling_avg_aqi"),
        col("rolling_avg_pm25"),
        col("rolling_avg_pm10"),
    )
)

# Register rolling aggregation as a temp view
rolling_aqi_df.createOrReplaceTempView("rolling_aqi")

# ── (b) Join with Static zone_profile + (c) Filter rolling_avg_aqi > 150 ─────
#
# Streaming–static join: zone_profile is a static DataFrame.
# Spark supports joining a streaming DataFrame with a static one.
#
enriched_df = (
    rolling_aqi_df
    .join(zone_profile_df, on="zone", how="left")
    # (c) Filter: only emit advisories for Unhealthy AQI zones
    .filter(col("rolling_avg_aqi") > AQI_ALERT_THRESHOLD)
    .select(
        col("zone"),
        col("zone_name"),
        col("population"),
        col("num_schools"),
        col("num_hospitals"),
        col("window_start"),
        col("window_end"),
        col("rolling_avg_aqi"),
        col("rolling_avg_pm25"),
        col("rolling_avg_pm10"),
        # Derive advisory level from AQI range (US EPA breakpoints)
        when(col("rolling_avg_aqi") > 300, lit("HAZARDOUS"))
        .when(col("rolling_avg_aqi") > 200, lit("VERY_UNHEALTHY"))
        .otherwise(lit("UNHEALTHY")).alias("advisory_level"),
        # Action message proportional to exposure risk
        when(col("rolling_avg_aqi") > 300,
             lit("Immediate evacuation recommended. All outdoor activity prohibited."))
        .when(col("rolling_avg_aqi") > 200,
             lit("Health alert: Everyone should avoid prolonged outdoor exposure."))
        .otherwise(
             lit("Sensitive groups should avoid outdoor activity. General public: limit exposure."))
        .alias("advisory_message"),
    )
)

# ── Sink: Write to Kafka with Update output mode ─────────────────────────────
kafka_output_df = enriched_df.select(
    col("zone").cast("string").alias("key"),
    to_json(struct(
        col("zone"),
        col("zone_name"),
        col("population"),
        col("num_schools"),
        col("num_hospitals"),
        col("window_start"),
        col("window_end"),
        col("rolling_avg_aqi"),
        col("rolling_avg_pm25"),
        col("rolling_avg_pm10"),
        col("advisory_level"),
        col("advisory_message"),
    )).alias("value")
)

query = (
    kafka_output_df.writeStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("topic",                   SINK_TOPIC)
    .option("checkpointLocation",      CHECKPOINT_PATH)
    .outputMode("update")    # Update: emit whenever a window result changes
    .trigger(processingTime="30 seconds")   # evaluate every 30 seconds
    .start()
)

logger.info(
    f"\n{'='*60}\n"
    f"Health Advisories Streaming SQL Job RUNNING\n"
    f"  Source  : {SOURCE_TOPIC}\n"
    f"  Window  : {WINDOW_DURATION} rolling (slide every {SLIDE_DURATION})\n"
    f"  Watermark: {WATERMARK_DELAY}\n"
    f"  Filter  : rolling_avg_aqi > {AQI_ALERT_THRESHOLD}\n"
    f"  Mode    : Update (Streaming SQL)\n"
    f"  Sink    : Kafka → {SINK_TOPIC}\n"
    f"{'='*60}"
)

query.awaitTermination()
