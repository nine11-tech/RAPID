"""
top10_malicious_ips.py
RAPID — Batch Layer
Top 10 des IPs sources malveillantes (threat_label IN ['suspicious', 'malicious'])
avec enrichissement regex (SQLi / XSS / Path-Traversal / Tool-Scan)
et calcul d'un score de réputation écrit dans HBase:ip_reputation.
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
HDFS_OUT  = f"{NAMENODE}/data/cybersecurity/batch/top10_malicious_ips"

# ── 1. Session Spark ──────────────────────────────────────────────────────────
spark = (
    SparkSession.builder
    .appName("RAPID-Top10MaliciousIPs")
    .config("spark.sql.shuffle.partitions", "8")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

print("=" * 60)
print("  RAPID — Top 10 Malicious IPs (Batch)")
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

# FIX: HDFS-first avec fallback local
try:
    df = spark.read.csv(HDFS_IN, header=True, schema=schema)
    df.take(1)
    print(f">>> Loaded from HDFS: {HDFS_IN}")
except Exception as e:
    print(f">>> HDFS unavailable ({e}), falling back to local: {LOCAL_CSV}")
    df = spark.read.csv(LOCAL_CSV, header=True, schema=schema)

total = df.count()
print(f">>> Total records: {total:,}")

# ── 3. Classification Regex ───────────────────────────────────────────────────
SQLI_PATTERN = (
    r"(?i)("
    r"'(\s|\+)*(or|and)(\s|\+)+'?[0-9]"
    r"|union(\s|\+)+select"
    r"|drop(\s|\+)+table"
    r"|insert(\s|\+)+into"
    r"|select(\s|\+)+\*"
    r"|--(\\s|$)"
    r"|;(\s)*drop"
    r"|xp_cmdshell"
    r"|information_schema"
    r")"
)
XSS_PATTERN = (
    r"(?i)("
    r"<script"
    r"|javascript:"
    r"|on(load|error|click|mouseover)="
    r"|<img[^>]+src"
    r"|alert\s*\("
    r"|document\.cookie"
    r"|eval\s*\("
    r")"
)
TRAVERSAL_PATTERN = r"(\.\./|\.\.\\|%2e%2e%2f|%252e)"
TOOL_PATTERN      = r"(?i)(sqlmap|nikto|nmap|masscan|dirbuster|gobuster|hydra|metasploit|burpsuite|acunetix)"

df_classified = (
    df
    .withColumn("is_sqli",      F.col("request_path").rlike(SQLI_PATTERN).cast("int"))
    .withColumn("is_xss",       F.col("request_path").rlike(XSS_PATTERN).cast("int"))
    .withColumn("is_traversal", F.col("request_path").rlike(TRAVERSAL_PATTERN).cast("int"))
    .withColumn("is_tool_scan", F.col("user_agent").rlike(TOOL_PATTERN).cast("int"))
)

# ── 4. Top 10 IPs malveillantes ───────────────────────────────────────────────
print("\n>>> TOP 10 MALICIOUS SOURCE IPs:")
top_ips = (
    df_classified
    .filter(F.col("threat_label").isin("suspicious", "malicious"))
    .groupBy("source_ip")
    .agg(
        F.count("*").alias("threat_count"),
        F.sum("is_sqli").alias("sqli_hits"),
        F.sum("is_xss").alias("xss_hits"),
        F.sum("is_traversal").alias("traversal_hits"),
        F.sum("is_tool_scan").alias("tool_hits"),
        F.avg("bytes_transferred").alias("avg_bytes"),
    )
    # FIX: score de réputation normalisé [0-100]
    .withColumn(
        "reputation_score",
        F.least(
            F.lit(100),
            (
                F.col("threat_count") * 10
                + F.col("sqli_hits") * 20
                + F.col("xss_hits") * 15
                + F.col("traversal_hits") * 15
                + F.col("tool_hits") * 25
            )
        ).cast("int")
    )
    .orderBy(F.col("reputation_score").desc())
    .limit(10)
)
top_ips.show(10, truncate=False)

# ── 5. Sauvegarde Parquet (HDFS) ──────────────────────────────────────────────
print(f"\n>>> Saving Parquet to: {HDFS_OUT}")
try:
    top_ips.write.mode("overwrite").parquet(HDFS_OUT)
    print(">>> Parquet saved!")
except Exception as e:
    print(f">>> Parquet write skipped (HDFS unavailable): {e}")

# ── 6. Écriture HBase (ip_reputation) ────────────────────────────────────────
print(f"\n>>> Writing top_ips to HBase @ {CHAWI_IP}:9090 (table: ip_reputation) ...")
top_ips_rows = top_ips.collect()

try:
    import happybase
    conn = happybase.Connection(CHAWI_IP, port=9090)
    tbl  = conn.table('ip_reputation')
    for row in top_ips_rows:
        rk = str(row['source_ip'])
        tbl.put(rk.encode(), {
            b'cf:threat_count':    str(row['threat_count']).encode(),
            b'cf:sqli_hits':       str(row['sqli_hits']).encode(),
            b'cf:xss_hits':        str(row['xss_hits']).encode(),
            b'cf:traversal_hits':  str(row['traversal_hits']).encode(),
            b'cf:tool_hits':       str(row['tool_hits']).encode(),
            b'cf:avg_bytes':       str(row['avg_bytes']).encode(),
            # FIX: champ manquant dans la version originale
            b'cf:reputation_score': str(row['reputation_score']).encode(),
        })
    conn.close()
    print(">>> ip_reputation HBase done!")
except Exception as e:
    print(f">>> HBase write failed: {e}")

print("\n>>> top10_malicious_ips.py COMPLETE!")
spark.stop()
