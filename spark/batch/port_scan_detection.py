
"""
port_scan_detection.py
RAPID — Batch Layer
Détecte les scans de ports : plusieurs connexions TCP vers des ports
différents depuis la même IP source dans une fenêtre glissante de 5 minutes.
Résultats écrits dans HBase:port_scans.
"""

import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, LongType

# ── IPs Tailscale ─────────────────────────────────────────────────────────────
ANASS_IP = os.getenv("ANASS_IP", "100.73.216.115")
CHAWI_IP = os.getenv("CHAWI_IP", "100.97.208.110")

NAMENODE  = f"hdfs://{ANASS_IP}:9000"
CSV_PATH  = f"{NAMENODE}/logs/year=2024/month=*/data.csv"
LOCAL_CSV = "/home/jovyan/work/batch/cybersecurity_threat_detection_logs.csv"
HDFS_OUT  = f"{NAMENODE}/data/cybersecurity/batch/port_scans"

# ── 1. Session Spark ──────────────────────────────────────────────────────────
spark = (
    SparkSession.builder
    .appName("RAPID-PortScanDetection")
    .config("spark.sql.shuffle.partitions", "8")
    # FIX: nécessaire pour les window functions en batch sans Kafka
    .config("spark.sql.streaming.checkpointLocation", "/tmp/checkpoint")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

print("=" * 60)
print("  RAPID — Port Scan Detection (Batch)")
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
    df = spark.read.csv(CSV_PATH, header=True, schema=schema)
    df.take(1)
    print(f">>> Loaded from HDFS: {CSV_PATH}")
except Exception as e:
    print(f">>> HDFS unavailable ({e}), falling back to local: {LOCAL_CSV}")
    df = spark.read.csv(LOCAL_CSV, header=True, schema=schema)

total = df.count()
print(f">>> Total records: {total:,}")

# ── 4. Filtrer sur TCP uniquement ─────────────────────────────────────────────
tcp_logs = df.filter(F.upper(F.col("protocol")) == "TCP")
print(f">>> TCP records: {tcp_logs.count():,}")

# ── 5. Extraction du port de destination ─────────────────────────────────────
# FIX: La regex originale échoue si dest_ip est une IP pure sans port.
# On gère les formats: "10.0.0.1:8080", "10.0.0.1", "[::1]:443"
tcp_logs = tcp_logs.withColumn(
    "dest_port",
    F.when(
        # Format standard ip:port — dernier segment après ":"
        F.col("dest_ip").rlike(r"^[^:]+:\d+$"),
        F.regexp_extract(F.col("dest_ip"), r":(\d+)$", 1)
    ).when(
        # Pas de port → utiliser dest_ip entier comme discriminant
        ~F.col("dest_ip").contains(":"),
        F.col("dest_ip")
    ).otherwise(
        # IPv6 bracket format [::1]:443 → extraire le port
        F.regexp_extract(F.col("dest_ip"), r"\]:(\d+)$", 1)
    )
)

# ── 6. Fenêtre glissante de 5 minutes par IP source ──────────────────────────
# FIX: F.window() retourne un StructType{start, end} — on ne peut pas l'écrire
# directement en Parquet/HBase sans le flatter. On ajoute window_start.
port_scans = (
    tcp_logs
    .groupBy(
        F.col("source_ip"),
        F.window(F.col("timestamp"), "5 minutes").alias("win")
    )
    .agg(
        F.countDistinct("dest_port").alias("distinct_ports"),
        F.count("*").alias("total_connections"),
    )
    # FIX: seuil ajusté à >20 ports distincts (conforme au cahier des charges)
    .filter(F.col("distinct_ports") > 20)
    # FIX: extraire start/end de la window pour sérialisation
    .withColumn("window_start", F.col("win.start"))
    .withColumn("window_end",   F.col("win.end"))
    .drop("win")
    .orderBy(F.col("distinct_ports").desc())
)

print("\n>>> PORT SCAN RESULTS (distinct_ports > 20 in 5-min window):")
port_scans.show(20, truncate=False)

result_count = port_scans.count()
print(f">>> Suspicious IPs detected: {result_count}")

# ── 7. Sauvegarde Parquet (HDFS) ──────────────────────────────────────────────
print(f"\n>>> Saving Parquet to: {HDFS_OUT}")
try:
    port_scans.write.mode("overwrite").parquet(HDFS_OUT)
    print(">>> Parquet saved!")
except Exception as e:
    print(f">>> Parquet write skipped (HDFS unavailable): {e}")

# ── 8. Écriture HBase (port_scans) ───────────────────────────────────────────
print(f"\n>>> Writing port scans to HBase @ {CHAWI_IP}:9090 (table: port_scans) ...")

def write_port_scans_hbase(rows):
    import happybase
    conn = happybase.Connection(CHAWI_IP, port=9090)
    tbl  = conn.table('port_scans')
    for row in rows:
        # row key = source_ip|window_start (unique par IP + fenêtre)
        rk = f"{row['source_ip']}|{str(row['window_start'])}"
        tbl.put(rk.encode(), {
            b'cf:source_ip':              str(row['source_ip']).encode(),
            b'cf:distinct_ports':         str(row['distinct_ports']).encode(),
            b'cf:total_connections':      str(row['total_connections']).encode(),
            b'cf:window_start':           str(row['window_start']).encode(),
            b'cf:window_end':             str(row['window_end']).encode(),
        })
    conn.close()

try:
    port_scans.foreachPartition(write_port_scans_hbase)
    print(">>> Port scans saved to HBase!")
except Exception as e:
    print(f">>> HBase write failed: {e}")

print("\n>>> Port scan detection complete!")
spark.stop()
