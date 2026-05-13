#  **full replacement file** for:

# ```text
# spark/streaming/threat_score.py
# ```

# It fixes the old bug where `score` was calculated in a Python loop and then written with `lit(score)`, causing wrong scores for multiple IPs in the same micro-batch. It also includes suspicious/signature scoring and case-insensitive detection.

# ```python
#!/usr/bin/env python3
"""
RAPID Sprint 2 - Task 6
Corrected real-time composite threat scoring per IP.

Fixes:
- Score is calculated per IP row, not reused from the last Python loop row.
- suspicious_count is included in scoring.
- SQLMap, Nmap, PathTraversal, /etc/passwd, phpmyadmin, backup.sql are detected case-insensitively.
- blocked traffic alone is not automatically treated as brute force.
- New checkpoint path is used to avoid old Spark state/schema conflicts.

Table written:
    cybersecurity.threat_scores

Expected columns:
    source_ip text
    score int
    total_events int
    suspicious_count int
    malicious_count int
    last_seen text
"""

from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json,
    col,
    sum as _sum,
    count as _count,
    window,
    when,
    to_timestamp,
    lit,
    lower,
    coalesce,
    greatest,
    least,
)
from pyspark.sql.types import StructType, StringType, LongType


# ───────────────────────────────────────────────────────────
# Config
# ───────────────────────────────────────────────────────────

KAFKA_HOST = "100.73.216.115:9092"
CASSANDRA_HOST = "100.97.208.110"
TOPIC = "cybersecurity-logs"

WINDOW_SIZE = "30 seconds"
SLIDE_SIZE = "10 seconds"

CHECKPOINT_PATH = "/home/jovyan/work/streaming/chkpt_threat_v2"


# ───────────────────────────────────────────────────────────
# Schema
# ───────────────────────────────────────────────────────────

schema = StructType() \
    .add("source_ip", StringType()) \
    .add("dest_ip", StringType()) \
    .add("protocol", StringType()) \
    .add("action", StringType()) \
    .add("threat_label", StringType()) \
    .add("log_type", StringType()) \
    .add("bytes_transferred", LongType()) \
    .add("user_agent", StringType()) \
    .add("request_path", StringType()) \
    .add("timestamp", StringType())


# ───────────────────────────────────────────────────────────
# Spark session
# ───────────────────────────────────────────────────────────

spark = SparkSession.builder \
    .appName("RAPID_ThreatScore_v2") \
    .config("spark.cassandra.connection.host", CASSANDRA_HOST) \
    .config("spark.cassandra.connection.port", "9042") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("=" * 70)
print("  RAPID - Threat Score Detection v2")
print(f"  Kafka      : {KAFKA_HOST}")
print(f"  Topic      : {TOPIC}")
print(f"  Cassandra  : {CASSANDRA_HOST}:9042")
print(f"  Checkpoint : {CHECKPOINT_PATH}")
print("=" * 70)


# ───────────────────────────────────────────────────────────
# Read Kafka stream
# ───────────────────────────────────────────────────────────

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


# ───────────────────────────────────────────────────────────
# Parse JSON
# ───────────────────────────────────────────────────────────

logs = raw.selectExpr("CAST(value AS STRING) AS json_str") \
    .select(from_json(col("json_str"), schema).alias("data")) \
    .select("data.*") \
    .withColumn("event_time", to_timestamp(col("timestamp"))) \
    .filter(col("source_ip").isNotNull()) \
    .filter(col("event_time").isNotNull()) \
    .withWatermark("event_time", "2 minutes")


# ───────────────────────────────────────────────────────────
# Normalize text for case-insensitive detection
# ───────────────────────────────────────────────────────────

normalized = logs \
    .withColumn("ua_l", lower(coalesce(col("user_agent"), lit("")))) \
    .withColumn("path_l", lower(coalesce(col("request_path"), lit("")))) \
    .withColumn("label_l", lower(coalesce(col("threat_label"), lit("")))) \
    .withColumn("action_l", lower(coalesce(col("action"), lit(""))))


# ───────────────────────────────────────────────────────────
# Event-level flags
# ───────────────────────────────────────────────────────────

flagged = normalized \
    .withColumn(
        "is_signature",
        when(
            col("ua_l").rlike("sqlmap|nikto|nmap|masscan|hydra|metasploit"),
            1
        )
        .when(
            col("path_l").rlike(
                "union\\s+select|or\\s+1=1|drop\\s+table|insert\\s+into|xp_cmdshell"
            ),
            1
        )
        .when(
            col("path_l").contains("phpmyadmin")
            | col("path_l").contains("backup.sql")
            | col("path_l").contains("/etc/passwd")
            | col("path_l").contains("../")
            | col("path_l").contains("..\\"),
            1
        )
        .otherwise(0)
    ) \
    .withColumn(
        "is_brute_force",
        when(
            (col("action_l") == "blocked")
            & (
                col("path_l").contains("login")
                | col("path_l").contains("auth")
                | col("ua_l").contains("hydra")
            ),
            1
        ).otherwise(0)
    ) \
    .withColumn(
        "is_suspicious",
        when(col("label_l") == "suspicious", 1)
        .when(col("is_signature") == 1, 1)
        .otherwise(0)
    ) \
    .withColumn(
        "is_malicious",
        when(col("label_l") == "malicious", 1)
        .when(
            col("path_l").contains("/etc/passwd")
            | col("path_l").contains("../")
            | col("path_l").contains("..\\"),
            1
        )
        .otherwise(0)
    )


# ───────────────────────────────────────────────────────────
# Aggregate per IP per time window
# ───────────────────────────────────────────────────────────

aggregated = flagged \
    .groupBy(
        col("source_ip"),
        window(col("event_time"), WINDOW_SIZE, SLIDE_SIZE)
    ) \
    .agg(
        _count("source_ip").alias("total_events"),
        _sum("is_signature").alias("signature_count"),
        _sum("is_suspicious").alias("suspicious_count"),
        _sum("is_malicious").alias("malicious_count"),
        _sum("is_brute_force").alias("bf_count"),
        (_sum("bytes_transferred") / 1048576).alias("volume_mb")
    )


# ───────────────────────────────────────────────────────────
# Per-row scoring
# ───────────────────────────────────────────────────────────

def score_dataframe(df):
    """
    Calculate score per IP row.

    Score components:
      signature score   = 35 + signature_count * 8
      suspicious score  = 25 + suspicious_count * 6
      malicious score   = 60 + malicious_count * 12
      brute force score = 20 + bf_count * 3
      volume score      = volume_mb * 2

    Final score:
      max(component scores), capped at 100
    """

    signature_score = when(
        col("signature_count") > 0,
        lit(35) + (col("signature_count") * lit(8))
    ).otherwise(lit(0))

    suspicious_score = when(
        col("suspicious_count") > 0,
        lit(25) + (col("suspicious_count") * lit(6))
    ).otherwise(lit(0))

    malicious_score = when(
        col("malicious_count") > 0,
        lit(60) + (col("malicious_count") * lit(12))
    ).otherwise(lit(0))

    brute_force_score = when(
        col("bf_count") > 0,
        lit(20) + (col("bf_count") * lit(3))
    ).otherwise(lit(0))

    volume_score = (col("volume_mb") * lit(2)).cast("int")

    return df.withColumn(
        "score",
        least(
            lit(100),
            greatest(
                signature_score,
                suspicious_score,
                malicious_score,
                brute_force_score,
                volume_score
            ).cast("int")
        )
    )


# ───────────────────────────────────────────────────────────
# Write to Cassandra
# ───────────────────────────────────────────────────────────

def write_threat_scores(batch_df, batch_id):
    count_val = batch_df.count()

    if count_val == 0:
        print(f">>> Batch {batch_id}: no threats scored")
        return

    scored_df = score_dataframe(batch_df)
    last_seen = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    print(f"\n>>> THREAT SCORES v2 — Batch {batch_id}: {count_val} IPs scored")

    scored_df.select(
        "source_ip",
        "total_events",
        "signature_count",
        "suspicious_count",
        "malicious_count",
        "bf_count",
        "volume_mb",
        "score"
    ).show(truncate=False)

    cassandra_df = scored_df.select(
        col("source_ip"),
        col("score").cast("int"),
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


# ───────────────────────────────────────────────────────────
# Start streaming query
# ───────────────────────────────────────────────────────────

query = aggregated.writeStream \
    .outputMode("update") \
    .foreachBatch(write_threat_scores) \
    .option("checkpointLocation", CHECKPOINT_PATH) \
    .trigger(processingTime="30 seconds") \
    .start()

print(">>> Stream started — scoring IPs in real time...")
print(">>> Formula v2:")
print("    signature:    35 + signature_count * 8")
print("    suspicious:   25 + suspicious_count * 6")
print("    malicious:    60 + malicious_count * 12")
print("    brute force:  20 + bf_count * 3")
print("    volume:       volume_mb * 2")
print("    final score = max(component scores), capped at 100")
print(">>> Ctrl+C to stop\n")

query.awaitTermination()
# ```

# Tell him after replacing the file:

# ```bash
# python3 -m py_compile spark/streaming/threat_score.py
# ```

# Then restart only this streaming job. Do **not** reuse the old checkpoint path. The file already uses:

# ```text
# /home/jovyan/work/streaming/chkpt_threat_v2
# ```