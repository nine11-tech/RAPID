"""
port_scan_detection.py
RAPID — Batch Layer
Détecte les scans de ports : plusieurs connexions TCP vers des ports
différents depuis la même IP source dans une fenêtre de 5 minutes.
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, LongType

# ── 1. Session Spark ─────────────────────────────────────────────────────────
spark = (
    SparkSession.builder
    .appName("RAPID-PortScanDetection")
    .config("spark.sql.shuffle.partitions", "8")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

print("=" * 60)
print("  RAPID — Port Scan Detection (Batch)")
print("=" * 60)

# ── 2. Lecture du CSV depuis HDFS (ou chemin local pour tests) ───────────────
CSV_PATH = "hdfs://namenode:9000/logs/year=2024/month=01/cybersecurity_threat_detection_logs.csv"
LOCAL_CSV = "/home/jovyan/work/data/raw/cybersecurity_threat_detection_logs.csv"

schema = StructType([
    StructField("timestamp",        TimestampType(), True),
    StructField("source_ip",        StringType(),    True),
    StructField("dest_ip",          StringType(),    True),
    StructField("protocol",         StringType(),    True),
    StructField("action",           StringType(),    True),
    StructField("threat_label",     StringType(),    True),
    StructField("log_type",         StringType(),    True),
    StructField("bytes_transferred",LongType(),      True),
    StructField("user_agent",       StringType(),    True),
    StructField("request_path",     StringType(),    True),
])

# Try HDFS first, fall back to local
try:
    df = spark.read.csv(CSV_PATH, header=True, schema=schema)
    df.take(1)  # test connectivity
    print(f">>> Loaded from HDFS: {CSV_PATH}")
except Exception:
    df = spark.read.csv(LOCAL_CSV, header=True, schema=schema)
    print(f">>> Loaded from local path: {LOCAL_CSV}")

total = df.count()
print(f">>> Total records: {total:,}")

# ── 3. Filtrer sur TCP uniquement ────────────────────────────────────────────
tcp_logs = df.filter(F.upper(F.col("protocol")) == "TCP")
print(f">>> TCP records: {tcp_logs.count():,}")

# ── 4. Extraire le port de destination depuis dest_ip (format ip:port) ───────
# Si dest_ip = "10.0.0.1:8080" → dest_port = "8080"
# Si pas de port, on utilise dest_ip entier comme discriminant
tcp_logs = tcp_logs.withColumn(
    "dest_port",
    F.when(
        F.col("dest_ip").contains(":"),
        F.split(F.col("dest_ip"), ":").getItem(1)
    ).otherwise(F.col("dest_ip"))
)

# ── 5. Fenêtre glissante de 5 minutes par IP source ─────────────────────────
port_scans = (
    tcp_logs
    .groupBy(
        F.col("source_ip"),
        F.window(F.col("timestamp"), "5 minutes")
    )
    .agg(
    F.countDistinct("dest_port").alias("distinct_ports"),
    F.count("*").alias("total_connections"),
    )
    .filter(F.col("distinct_ports") > 20)
    .orderBy(F.col("distinct_ports").desc())
)

print("\n>>> PORT SCAN RESULTS (distinct_ports > 20 in 5-min window):")
port_scans.show(20, truncate=False)

result_count = port_scans.count()
print(f">>> Suspicious IPs detected: {result_count}")

# ── 6. Sauvegarde en Parquet ─────────────────────────────────────────────────
OUTPUT_PATH = "hdfs://namenode:9000/data/cybersecurity/batch/port_scans"
LOCAL_OUTPUT = "/home/jovyan/work/data/batch_output/port_scans"

try:
    port_scans.write.mode("overwrite").parquet(OUTPUT_PATH)
    print(f">>> Results saved to HDFS: {OUTPUT_PATH}")
except Exception as e:
    print(f">>> HDFS write failed ({e}), saving locally...")
    port_scans.write.mode("overwrite").parquet(LOCAL_OUTPUT)
    print(f">>> Results saved locally: {LOCAL_OUTPUT}")

print("\n>>> Port scan detection complete!")
spark.stop()