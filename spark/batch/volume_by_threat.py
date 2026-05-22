"""
volume_by_threat.py
RAPID — Batch Layer
Corrélation bytes_transferred ↔ threat_label et ↔ log_type.
Résultats écrits dans HBase:attack_patterns (préfixe THREAT_VOL|).
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
HDFS_OUT  = f"{NAMENODE}/data/cybersecurity/batch/volume_by_threat"

# ── 1. Session Spark ──────────────────────────────────────────────────────────
spark = (
    SparkSession.builder
    .appName("RAPID-VolumeByThreat")
    .config("spark.sql.shuffle.partitions", "8")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

print("=" * 60)
print("  RAPID — Volume by Threat (Batch)")
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
try:
    df = spark.read.csv(HDFS_IN, header=True, schema=schema)
    df.take(1)
    print(f">>> Loaded from HDFS: {HDFS_IN}")
except Exception as e:
    print(f">>> HDFS unavailable ({e}), falling back to local: {LOCAL_CSV}")
    df = spark.read.csv(LOCAL_CSV, header=True, schema=schema)

total = df.count()
print(f">>> Total records: {total:,}")

# ── 4. Corrélation bytes_transferred ↔ threat_label ──────────────────────────
print("\n>>> BYTES TRANSFERRED BY THREAT LABEL:")
bytes_by_threat = (
    df
    .groupBy("threat_label")
    .agg(
        F.count("*").alias("count"),
        F.avg("bytes_transferred").alias("avg_bytes"),
        F.min("bytes_transferred").alias("min_bytes"),
        F.max("bytes_transferred").alias("max_bytes"),
        F.sum("bytes_transferred").alias("total_bytes"),
        F.stddev("bytes_transferred").alias("stddev_bytes"),
        # FIX: percentile utile pour détecter les exfiltrations volumineuses
        F.percentile_approx("bytes_transferred", 0.95).alias("p95_bytes"),
    )
    .orderBy("threat_label")
)
bytes_by_threat.show(truncate=False)

# ── 5. Corrélation bytes_transferred ↔ log_type ──────────────────────────────
print("\n>>> BYTES TRANSFERRED BY LOG TYPE:")
bytes_by_logtype = (
    df
    .groupBy("log_type")
    .agg(
        F.count("*").alias("occurrence_count"),
        F.avg("bytes_transferred").alias("avg_bytes"),
        F.sum("bytes_transferred").alias("total_bytes"),
    )
    .orderBy(F.col("total_bytes").desc())
)
bytes_by_logtype.show(truncate=False)

# ── 6. Corrélation bytes_transferred ↔ threat_label × protocol ───────────────
# FIX: analyse croisée manquante dans la version originale
print("\n>>> BYTES BY THREAT LABEL x PROTOCOL:")
bytes_by_threat_proto = (
    df
    .groupBy("threat_label", "protocol")
    .agg(
        F.count("*").alias("count"),
        F.sum("bytes_transferred").alias("total_bytes"),
        F.avg("bytes_transferred").alias("avg_bytes"),
    )
    .orderBy(F.col("total_bytes").desc())
)
bytes_by_threat_proto.show(30, truncate=False)

# ── 7. Sauvegarde Parquet (HDFS) ──────────────────────────────────────────────
print(f"\n>>> Saving Parquet to: {HDFS_OUT}")
try:
    bytes_by_threat.write.mode("overwrite").parquet(HDFS_OUT)
    print(">>> Parquet saved!")
except Exception as e:
    print(f">>> Parquet write skipped (HDFS unavailable): {e}")

# ── 8. Écriture HBase (threat_volume) ────────────────────────────────────────
# FIX 1: table 'threat_volume' (pas 'attack_patterns')
# FIX 2: collect() sur le driver + connexion par row (évite TSocket crash)
print(f"\n>>> Writing bytes_by_threat to HBase @ {CHAWI_IP}:9090 (table: threat_volume) ...")

try:
    import happybase
    rows = bytes_by_threat.collect()
    print(f">>> Writing {len(rows)} rows to HBase...")
    for row in rows:
        conn = happybase.Connection(CHAWI_IP, port=9090)
        tbl  = conn.table('threat_volume')
        rk   = f"THREAT_VOL|{row['threat_label']}"
        tbl.put(rk.encode(), {
            b'cf:threat_label': str(row['threat_label']).encode(),
            b'cf:total_bytes':  str(row['total_bytes']).encode(),
            b'cf:avg_bytes':    str(row['avg_bytes']).encode(),
            b'cf:min_bytes':    str(row['min_bytes']).encode(),
            b'cf:max_bytes':    str(row['max_bytes']).encode(),
            b'cf:stddev_bytes': str(row['stddev_bytes']).encode(),
            b'cf:p95_bytes':    str(row['p95_bytes']).encode(),
            b'cf:count':        str(row['count']).encode(),
        })
        conn.close()
        print(f"    Written: {rk}")
    print(">>> threat_volume HBase done!")
except Exception as e:
    print(f">>> HBase write failed: {e}")

print("\n>>> volume_by_threat.py COMPLETE!")
spark.stop()
