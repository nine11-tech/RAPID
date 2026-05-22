"""
hbase_threat_timeline.py
RAPID — Batch Layer
Évolution temporelle des menaces : agrégation par heure et par jour.
Résultats écrits dans HBase:threat_timeline.
"""

import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, LongType

# ── IPs Tailscale ─────────────────────────────────────────────────────────────
ANASS_IP = os.getenv("ANASS_IP", "100.73.216.115")
CHAWI_IP = os.getenv("CHAWI_IP", "100.97.208.110")

NAMENODE  = f"hdfs://{ANASS_IP}:9000"
HDFS_IN   = f"{NAMENODE}/logs/year=2024/month=*/data.csv"
LOCAL_CSV = "/home/jovyan/work/batch/cybersecurity_threat_detection_logs.csv" 
HDFS_OUT  = f"{NAMENODE}/data/cybersecurity/batch/threat_timeline"

# ── 1. Session Spark ──────────────────────────────────────────────────────────
spark = (
    SparkSession.builder
    .appName("RAPID-HBaseThreatTimeline")
    .config("spark.sql.shuffle.partitions", "8")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

print("=" * 60)
print("  RAPID — HBase Threat Timeline (Batch)")
print("=" * 60)

# ── 2. Schéma explicite ───────────────────────────────────────────────────────
schema = StructType([
    StructField("timestamp",         TimestampType(), True),
    StructField("source_ip",         StringType(),    True),
    StructField("dest_ip",           StringType(),    True),
    StructField("protocol",          StringType(),    True),
    StructField("action",            StringType(),    True),
    StructField("threat_label",      StringType(),    True),
    StructField("log_type",          StringType(),    True),
    StructField("bytes_transferred", LongType(),      True),
    StructField("user_agent",        StringType(),    True),
    StructField("request_path",      StringType(),    True),
])

# ── 3. Lecture HDFS avec fallback local ──────────────────────────────────────
print(">>> Loading dataset...")
try:
    df = spark.read.csv(HDFS_IN, header=True, schema=schema)
    df.take(1)
    print(f">>> Loaded from HDFS: {HDFS_IN}")
except Exception as e:
    print(f">>> HDFS unavailable ({e}), falling back to local: {LOCAL_CSV}")
    df = spark.read.csv(LOCAL_CSV, header=True, schema=schema)

total = df.count()
print(f">>> Total records: {total:,}")

df = df.filter(F.col("timestamp").isNotNull())

# ── 4. Agrégation par heure + threat_label ────────────────────────────────────
print(">>> Aggregating by hour...")
timeline_hourly = (
    df
    .groupBy(
        F.date_trunc("hour", F.col("timestamp")).alias("heure"),
        F.col("threat_label")
    )
    .agg(
        F.count("*").alias("event_count"),
        F.countDistinct("source_ip").alias("distinct_ips"),
        F.sum("bytes_transferred").alias("total_bytes"),
    )
    .orderBy("heure", "threat_label")
)

print("\n=== THREAT TIMELINE (par heure) ===")
timeline_hourly.show(20, truncate=False)

# ── 5. Agrégation par jour + threat_label ─────────────────────────────────────
timeline_daily = (
    df
    .groupBy(
        F.date_trunc("day", F.col("timestamp")).alias("jour"),
        F.col("threat_label")
    )
    .agg(
        F.count("*").alias("event_count"),
        F.countDistinct("source_ip").alias("distinct_ips"),
        F.sum("bytes_transferred").alias("total_bytes"),
    )
    .orderBy("jour", "threat_label")
)

print("\n=== THREAT TIMELINE (par jour) ===")
timeline_daily.show(20, truncate=False)

# ── 6. Sauvegarde Parquet (HDFS) ──────────────────────────────────────────────
print(f"\n>>> Saving Parquet to: {HDFS_OUT}")
try:
    timeline_hourly.write.mode("overwrite").parquet(f"{HDFS_OUT}/hourly")
    timeline_daily.write.mode("overwrite").parquet(f"{HDFS_OUT}/daily")
    print(">>> Parquet saved!")
except Exception as e:
    print(f">>> Parquet write skipped (HDFS unavailable): {e}")

# ── 7. Écriture dans HBase (threat_timeline) ──────────────────────────────────
# FIX 2: collect() on the driver + one happybase connection for the whole batch,
# instead of foreachPartition which opens a connection per partition and causes
# TSocket crashes when thousands of rows fan out across many partitions.

print(">>> Writing to HBase...")

def write_rows_to_hbase(rows_iter, granularity):
    """
    rows_iter : list of Row objects (collected on the driver)
    granularity: "HOURLY" or "DAILY"
    """
    import happybase
    connection = happybase.Connection(CHAWI_IP, port=9090)
    table = connection.table('threat_timeline')
    try:
        batch = table.batch(batch_size=500)          # single TCP stream, no per-row round-trip
        for row in rows_iter:
            if granularity == "HOURLY":
                ts_val      = str(row['heure'])
                ts_col_key  = b'cf:heure'
                ts_col_val  = ts_val.encode()
            else:
                ts_val      = str(row['jour'])
                ts_col_key  = b'cf:jour'
                ts_col_val  = ts_val.encode()

            row_key = f"{granularity}|{ts_val}|{row['threat_label']}"
            batch.put(row_key.encode(), {
                ts_col_key:         ts_col_val,
                b'cf:threat_label': str(row['threat_label']).encode(),
                b'cf:event_count':  str(row['event_count']).encode(),
                b'cf:distinct_ips': str(row['distinct_ips']).encode(),
                b'cf:total_bytes':  str(row['total_bytes']).encode(),
            })
        batch.send()
    finally:
        connection.close()

try:
    hourly_rows = timeline_hourly.collect()
    write_rows_to_hbase(hourly_rows, "HOURLY")
    print(f">>> HBase threat_timeline (hourly) populated! ({len(hourly_rows)} rows)")
except Exception as e:
    print(f">>> HBase hourly write failed: {e}")

try:
    daily_rows = timeline_daily.collect()
    write_rows_to_hbase(daily_rows, "DAILY")
    print(f">>> HBase threat_timeline (daily) populated! ({len(daily_rows)} rows)")
except Exception as e:
    print(f">>> HBase daily write failed: {e}")

print("\n>>> hbase_threat_timeline.py COMPLETE!")
spark.stop()
