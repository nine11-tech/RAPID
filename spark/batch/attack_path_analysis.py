"""
attack_path_analysis.py
RAPID — Batch Layer
Extraction de patterns SQLi / XSS / Path-Traversal / Tool-Scan depuis request_path
et user_agent. Résultats écrits dans HBase:attack_patterns.
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
HDFS_OUT  = f"{NAMENODE}/data/cybersecurity/batch/attack_patterns"

# ── 1. Session Spark ──────────────────────────────────────────────────────────
spark = (
    SparkSession.builder
    .appName("RAPID-AttackPathAnalysis")
    .config("spark.sql.shuffle.partitions", "8")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

print("=" * 60)
print("  RAPID — Attack Path Analysis (Batch)")
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

# ── 4. Signatures d'attaques ──────────────────────────────────────────────────
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
# FIX: La regex XSS originale avait une quote mal échappée dans on(load|...)=
# qui cassait le parsing Python. Corrigé avec raw-string propre.
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
        # FIX: ajout colonnes utiles manquantes
        F.countDistinct("source_ip").alias("distinct_ips"),
        F.max("timestamp").alias("last_seen"),
    )
    .orderBy(F.col("occurrences").desc())
)
attack_summary.show(30, truncate=False)

# ── 7. Top IPs par type d'attaque ─────────────────────────────────────────────
print("\n>>> TOP SOURCE IPs PER ATTACK TYPE:")
top_ips_per_attack = (
    df_classified
    .filter(F.col("attack_type") != "Unknown")
    .groupBy("attack_type", "source_ip")
    .agg(F.count("*").alias("hits"))
    .withColumn(
        "rank",
        F.row_number().over(
            __import__("pyspark.sql.window", fromlist=["Window"])
            .Window.partitionBy("attack_type").orderBy(F.col("hits").desc())
        )
    )
    .filter(F.col("rank") <= 5)
    .orderBy("attack_type", "rank")
)
top_ips_per_attack.show(50, truncate=False)

# ── 8. Sauvegarde Parquet (HDFS) ──────────────────────────────────────────────
print(f"\n>>> Saving Parquet to: {HDFS_OUT}")
try:
    attack_summary.write.mode("overwrite").parquet(HDFS_OUT)
    print(">>> Parquet saved!")
except Exception as e:
    print(f">>> Parquet write skipped (HDFS unavailable): {e}")

# ── 9. Écriture HBase (attack_patterns) ──────────────────────────────────────
# FIX: foreachPartition causait TSocket read 0 bytes sur les gros datasets
# car HBase fermait la connexion Thrift au milieu de l'écriture.
# Solution: collect() sur le driver (attack_summary = 5 rows max) + une
# connexion par row avec reconnexion automatique.
print(f"\n>>> Writing attack_patterns to HBase @ {CHAWI_IP}:9090 ...")

try:
    import happybase
    rows = attack_summary.collect()
    print(f">>> Writing {len(rows)} rows to HBase...")
    for row in rows:
        # FIX: nouvelle connexion par row pour éviter le timeout Thrift
        conn = happybase.Connection(CHAWI_IP, port=9090)
        tbl  = conn.table('attack_patterns')
        rk   = f"{row['attack_type']}|{row['threat_label']}"
        tbl.put(rk.encode(), {
            b'cf:attack_type':  str(row['attack_type']).encode(),
            b'cf:threat_label': str(row['threat_label']).encode(),
            b'cf:occurrences':  str(row['occurrences']).encode(),
            b'cf:avg_bytes':    str(row['avg_bytes']).encode(),
            b'cf:total_bytes':  str(row['total_bytes']).encode(),
            b'cf:distinct_ips': str(row['distinct_ips']).encode(),
            b'cf:last_seen':    str(row['last_seen']).encode(),
        })
        conn.close()
        print(f"    Written: {rk}")
    print(">>> attack_patterns HBase done!")
except Exception as e:
    print(f">>> HBase write failed: {e}")

print("\n>>> attack_path_analysis.py COMPLETE!")
spark.stop()
