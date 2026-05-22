#!/usr/bin/env python3
"""
RAPID Sprint 2 - Task 6 (Chawi)
Score de menace composite par IP en temps reel
bf*2 + sig*5 + vol*2 = score (0-100)
Table: cybersecurity.threat_scores
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, sum as _sum, count as _count, window, when,
    to_timestamp, lit
)
from pyspark.sql.types import (
    StructType, StringType, LongType, IntegerType
)
from datetime import datetime

# ── Config ────────────────────────────────────────────
KAFKA_HOST     = "100.73.216.115:9092"
CASSANDRA_HOST = "100.97.208.110"
TOPIC          = "cybersecurity-logs"
WINDOW_SIZE    = "30 seconds"
SLIDE_SIZE     = "10 seconds"

# ── Schema ────────────────────────────────────────────
schema = StructType() \
    .add("source_ip",         StringType()) \
    .add("dest_ip",           StringType()) \
    .add("protocol",          StringType()) \
    .add("action",            StringType()) \
    .add("threat_label",      StringType()) \
    .add("log_type",          StringType()) \
    .add("bytes_transferred", LongType()) \
    .add("user_agent",        StringType()) \
    .add("request_path",      StringType()) \
    .add("timestamp",         StringType())

# ── Spark Session ─────────────────────────────────────
spark = SparkSession.builder \
    .appName("Task6_ThreatScore") \
    .config("spark.cassandra.connection.host", CASSANDRA_HOST) \
    .config("spark.cassandra.connection.port", "9042") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")
print("=" * 60)
print("  RAPID - Threat Score Detection")
print(f"  Kafka     : {KAFKA_HOST}")
print(f"  Cassandra : {CASSANDRA_HOST}")
print("=" * 60)

# ── Read Kafka (same options as brute_force) ──────────
raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_HOST) \
    .option("subscribe", TOPIC) \
    .option("startingOffsets", "latest") \
    .option("maxOffsetsPerTrigger", 1000) \
    .option("failOnDataLoss", "false") \
    .option("kafka.request.timeout.ms", "120000") \
    .option("kafka.session.timeout.ms", "60000") \
    .load()

# ── Parse (same method as brute_force) ────────────────
logs = raw.selectExpr("CAST(value AS STRING) as json_str") \
    .select(from_json(col("json_str"), schema).alias("data")) \
    .select("data.*") \
    .withColumn("event_time", to_timestamp(col("timestamp"))) \
    .withWatermark("event_time", "2 minutes")

# ── Flag each event ───────────────────────────────────
flagged = logs \
    .withColumn("is_brute_force",
        when(col("action") == "blocked", 1).otherwise(0)
    ) \
    .withColumn("is_suspicious",
        when(col("threat_label") == "suspicious", 1)
        .when(col("user_agent").contains("sqlmap"), 1)
        .when(col("user_agent").contains("nikto"), 1)
        .when(col("request_path").contains("OR 1=1"), 1)
        .when(col("request_path").contains("DROP TABLE"), 1)
        .when(col("request_path").contains("phpmyadmin"), 1)
        .otherwise(0)
    ) \
    .withColumn("is_malicious",
        when(col("threat_label") == "malicious", 1).otherwise(0)
    )

# ── Aggregate per IP per window ───────────────────────
aggregated = flagged \
    .groupBy(
        col("source_ip"),
        window(col("event_time"), WINDOW_SIZE, SLIDE_SIZE)
    ) \
    .agg(
        _count("source_ip").alias("total_events"),
        _sum("is_suspicious").alias("suspicious_count"),
        _sum("is_malicious").alias("malicious_count"),
        _sum("is_brute_force").alias("bf_count"),
        (_sum("bytes_transferred") / 1048576).alias("volume_mb")
    )

# ── Write to Cassandra ────────────────────────────────
def write_threat_scores(batch_df, batch_id):
    count_val = batch_df.count()
    if count_val == 0:
        print(f">>> Batch {batch_id}: no threats scored")
        return

    print(f"\n>>> THREAT SCORES — Batch {batch_id}: {count_val} IPs scored")
    batch_df.show(truncate=False)

    rows = batch_df.collect()

    for row in rows:
        source_ip        = row["source_ip"]
        total_events     = int(row["total_events"]     or 0)
        suspicious_count = int(row["suspicious_count"] or 0)
        malicious_count  = int(row["malicious_count"]  or 0)
        bf_count         = int(row["bf_count"]         or 0)
        vol              = float(row["volume_mb"]       or 0)
        last_seen        = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        score            = min(100, (bf_count * 2) + (malicious_count * 5) + (int(vol) * 2))

        level = "CRITICAL" if score >= 80 else \
                "HIGH" if score >= 60 else \
                "MEDIUM" if score >= 40 else \
                "LOW" if score >= 20 else "INFO"

        print(f"🎯 {source_ip:<20} | TOT:{total_events:4} SUS:{suspicious_count:3} MAL:{malicious_count:3} | SCORE:{score:3} [{level}] | {last_seen}")

    # Write to Cassandra
    cassandra_df = batch_df.select(
        col("source_ip"),
        lit(score).cast("int").alias("score"),
        col("total_events").cast("int"),
        col("suspicious_count").cast("int"),
        col("malicious_count").cast("int"),
        lit(last_seen).alias("last_seen")
    )

    try:
        cassandra_df.write \
            .format("org.apache.spark.sql.cassandra") \
            .options(table="threat_scores", keyspace="cybersecurity") \
            .mode("append") \
            .save()
        print(f">>> {count_val} records written to cybersecurity.threat_scores")
    except Exception as e:
        print(f">>> Cassandra write error: {e}")

# ── Start Stream ──────────────────────────────────────
query = aggregated.writeStream \
    .outputMode("update") \
    .foreachBatch(write_threat_scores) \
    .option("checkpointLocation", "/home/jovyan/work/streaming/chkpt_threat") \
    .trigger(processingTime="30 seconds") \
    .start()

print(">>> Stream started — scoring IPs in real time...")
print(">>> Formula: bf*2 + malicious*5 + vol*2 (max 100)")
print(">>> Ctrl+C to stop\n")
query.awaitTermination()
