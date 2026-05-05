"""
attack_pattern_detection.py
RAPID — Batch Layer
Extrait les patterns SQLi / XSS depuis request_path et calcule
la corrélation bytes_transferred ↔ threat_label.

Run depuis spark-master (Hamza):
  spark-submit \
    --master spark://100.72.34.26:7077 \
    --packages org.apache.spark:spark-sql_2.12:3.x \
    attack_pattern_detection.py
"""

import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, LongType

# ── IPs Tailscale (injectées via .env ou variables d'environnement) ──────────
ANASS_IP  = os.getenv("ANASS_IP",  "100.73.216.115")
CHAWI_IP  = os.getenv("CHAWI_IP",  "100.97.208.110")

NAMENODE  = f"hdfs://{ANASS_IP}:9000"
HDFS_IN   = f"{NAMENODE}/logs/year=2024/month=*/data.csv"
HDFS_OUT  = f"{NAMENODE}/data/cybersecurity/batch"

# ── 1. Session Spark ─────────────────────────────────────────────────────────
# Ne pas hardcoder .master() — spark-submit gère ça
spark = (
    SparkSession.builder
    .appName("RAPID-AttackPatternDetection")
    .config("spark.sql.shuffle.partitions", "8")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

print("=" * 60)
print("  RAPID — Attack Pattern Detection (Batch)")
print(f"  HDFS: {HDFS_IN}")
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

# ── 3. Lecture HDFS (tous les mois) ──────────────────────────────────────────
print(f">>> Loading from HDFS: {HDFS_IN}")
df = spark.read.csv(HDFS_IN, header=True, schema=schema)
total = df.count()
print(f">>> Total records: {total:,}")

# ── 4. Signatures d'attaques ──────────────────────────────────────────────────
SQLI_PATTERN = (
    r"(?i)("
    r"'(\s|\+)*(or|and)(\s|\+)+'?[0-9]"
    r"|union(\s|\+)+select"
    r"|drop(\s|\+)+table"
    r"|insert(\s|\+)+into"
    r"|select(\s|\+)+\*"
    r"|--(\s|$)"
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

# ── 5. Classification ─────────────────────────────────────────────────────────
df_classified = (
    df
    .withColumn("is_sqli",      F.col("request_path").rlike(SQLI_PATTERN).cast("int"))
    .withColumn("is_xss",       F.col("request_path").rlike(XSS_PATTERN).cast("int"))
    .withColumn("is_traversal", F.col("request_path").rlike(TRAVERSAL_PATTERN).cast("int"))
    .withColumn("is_tool_scan", F.col("user_agent").rlike(TOOL_PATTERN).cast("int"))
    .withColumn(
        "attack_type",
        F.when(F.col("is_sqli")      == 1, "SQLi")
         .when(F.col("is_xss")       == 1, "XSS")
         .when(F.col("is_traversal") == 1, "PathTraversal")
         .when(F.col("is_tool_scan") == 1, "ToolScan")
         .otherwise("Unknown")
    )
)

# ── 6. Résumé par type d'attaque ──────────────────────────────────────────────
print("\n>>> ATTACK TYPE SUMMARY:")
attack_summary = (
    df_classified
    .filter(F.col("attack_type") != "Unknown")
    .groupBy("attack_type", "threat_label")
    .agg(
        F.count("*").alias("occurrences"),
        F.avg("bytes_transferred").alias("avg_bytes"),
        F.sum("bytes_transferred").alias("total_bytes"),
    )
    .orderBy(F.col("occurrences").desc())
)
attack_summary.show(30, truncate=False)

# ── 7. Top 10 IPs malveillantes ───────────────────────────────────────────────
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
    .orderBy(F.col("threat_count").desc())
    .limit(10)
)
top_ips.show(10, truncate=False)

# ── 8. Corrélation bytes_transferred ↔ threat_label ──────────────────────────
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
    )
    .orderBy("threat_label")
)
bytes_by_threat.show(truncate=False)

# ── 9. Corrélation bytes_transferred ↔ log_type ───────────────────────────────
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

# ── 10. Top SQLi paths ────────────────────────────────────────────────────────
print("\n>>> TOP SQLi REQUEST PATHS:")
top_sqli = (
    df_classified
    .filter(F.col("is_sqli") == 1)
    .groupBy("request_path", "threat_label")
    .count()
    .orderBy(F.col("count").desc())
    .limit(20)
)
top_sqli.show(20, truncate=False)

# ── 11. Sauvegarde Parquet → HDFS (Anass) ─────────────────────────────────────
def save_parquet(df_to_save, path, name):
    df_to_save.write.mode("overwrite").parquet(path)
    print(f">>> [{name}] saved → {path}")

save_parquet(attack_summary,    f"{HDFS_OUT}/attack_patterns",      "attack_patterns")
save_parquet(top_ips,           f"{HDFS_OUT}/top_malicious_ips",    "top_malicious_ips")
save_parquet(bytes_by_threat,   f"{HDFS_OUT}/bytes_by_threat",      "bytes_by_threat")
save_parquet(bytes_by_logtype,  f"{HDFS_OUT}/bytes_by_logtype",     "bytes_by_logtype")
save_parquet(
    df_classified.filter(F.col("attack_type") != "Unknown"),
    f"{HDFS_OUT}/classified_attacks",
    "classified_attacks"
)

# ── 12. Écriture HBase → Chawi (100.97.208.110) ───────────────────────────────
# Table: attack_patterns   row_key: attack_type|threat_label
print(f"\n>>> Writing attack_patterns to HBase @ {CHAWI_IP}:9090 ...")

def write_attack_patterns_hbase(rows):
    import happybase
    conn = happybase.Connection(CHAWI_IP, port=9090)
    tbl  = conn.table('attack_patterns')
    for row in rows:
        rk = f"{row['attack_type']}|{row['threat_label']}"
        tbl.put(rk.encode(), {
            b'cf:attack_type':  str(row['attack_type']).encode(),
            b'cf:threat_label': str(row['threat_label']).encode(),
            b'cf:occurrences':  str(row['occurrences']).encode(),
            b'cf:avg_bytes':    str(row['avg_bytes']).encode(),
            b'cf:total_bytes':  str(row['total_bytes']).encode(),
        })
    conn.close()

# Table: ip_reputation   row_key: source_ip
print(f">>> Writing ip_reputation to HBase @ {CHAWI_IP}:9090 ...")

def write_ip_reputation_hbase(rows):
    import happybase
    conn = happybase.Connection(CHAWI_IP, port=9090)
    tbl  = conn.table('ip_reputation')
    for row in rows:
        rk = str(row['source_ip'])
        tbl.put(rk.encode(), {
            b'cf:threat_count':    str(row['threat_count']).encode(),
            b'cf:sqli_hits':       str(row['sqli_hits']).encode(),
            b'cf:xss_hits':        str(row['xss_hits']).encode(),
            b'cf:traversal_hits':  str(row['traversal_hits']).encode(),
            b'cf:tool_hits':       str(row['tool_hits']).encode(),
            b'cf:avg_bytes':       str(row['avg_bytes']).encode(),
        })
    conn.close()

attack_summary.foreachPartition(write_attack_patterns_hbase)
print(">>> attack_patterns HBase done!")

# top_ips n'est qu'un limit(10) — collect() + boucle simple est OK ici
top_ips_rows = top_ips.collect()
import happybase
conn = happybase.Connection(CHAWI_IP, port=9090)
tbl  = conn.table('ip_reputation')
for row in top_ips_rows:
    rk = str(row['source_ip'])
    tbl.put(rk.encode(), {
        b'cf:threat_count':   str(row['threat_count']).encode(),
        b'cf:sqli_hits':      str(row['sqli_hits']).encode(),
        b'cf:xss_hits':       str(row['xss_hits']).encode(),
        b'cf:traversal_hits': str(row['traversal_hits']).encode(),
        b'cf:tool_hits':      str(row['tool_hits']).encode(),
        b'cf:avg_bytes':      str(row['avg_bytes']).encode(),
    })
conn.close()
print(">>> ip_reputation HBase done!")

print("\n>>> attack_pattern_detection.py COMPLETE!")
spark.stop()