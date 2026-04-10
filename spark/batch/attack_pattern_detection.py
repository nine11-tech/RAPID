"""
attack_pattern_detection.py
RAPID — Batch Layer
Extrait les patterns SQLi / XSS depuis request_path et calcule
la corrélation bytes_transferred ↔ threat_label.
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, LongType

# ── 1. Session Spark ─────────────────────────────────────────────────────────
spark = (
    SparkSession.builder
    .appName("RAPID-AttackPatternDetection")
    .config("spark.sql.shuffle.partitions", "8")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

print("=" * 60)
print("  RAPID — Attack Pattern Detection (Batch)")
print("=" * 60)

# ── 2. Lecture CSV ────────────────────────────────────────────────────────────
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

try:
    df = spark.read.csv(CSV_PATH, header=True, schema=schema)
    df.take(1)
    print(f">>> Loaded from HDFS: {CSV_PATH}")
except Exception:
    df = spark.read.csv(LOCAL_CSV, header=True, schema=schema)
    print(f">>> Loaded from local path: {LOCAL_CSV}")

total = df.count()
print(f">>> Total records: {total:,}")

# ── 3. Signatures d'attaques ─────────────────────────────────────────────────
# SQLi patterns
SQLI_PATTERN = (
    r"(?i)("
    r"'(\s|\+)*(or|and)(\s|\+)+'?[0-9]"       # ' OR 1=1
    r"|union(\s|\+)+select"                    # UNION SELECT
    r"|drop(\s|\+)+table"                      # DROP TABLE
    r"|insert(\s|\+)+into"                     # INSERT INTO
    r"|select(\s|\+)+\*"                       # SELECT *
    r"|--(\s|$)"                               # SQL comment --
    r"|;(\s)*drop"                             # ; DROP
    r"|xp_cmdshell"                            # SQL Server shell
    r"|information_schema"                     # schema enumeration
    r")"
)

# XSS patterns
XSS_PATTERN = (
    r"(?i)("
    r"<script"                                 # <script>
    r"|javascript:"                            # javascript: URI
    r"|on(load|error|click|mouseover)="        # event handlers
    r"|<img[^>]+src"                           # <img src=
    r"|alert\s*\("                             # alert(
    r"|document\.cookie"                       # cookie theft
    r"|eval\s*\("                              # eval(
    r")"
)

# Path traversal / other patterns
TRAVERSAL_PATTERN = r"(\.\./|\.\.\\|%2e%2e%2f|%252e)"

# Tool signatures in user_agent
TOOL_PATTERN = r"(?i)(sqlmap|nikto|nmap|masscan|dirbuster|gobuster|hydra|metasploit|burpsuite|acunetix)"

# ── 4. Classement des requêtes par type d'attaque ────────────────────────────
df_classified = df.withColumn(
    "is_sqli",      F.col("request_path").rlike(SQLI_PATTERN).cast("int")
).withColumn(
    "is_xss",       F.col("request_path").rlike(XSS_PATTERN).cast("int")
).withColumn(
    "is_traversal", F.col("request_path").rlike(TRAVERSAL_PATTERN).cast("int")
).withColumn(
    "is_tool_scan", F.col("user_agent").rlike(TOOL_PATTERN).cast("int")
).withColumn(
    "attack_type",
    F.when(F.col("is_sqli")      == 1, "SQLi")
     .when(F.col("is_xss")       == 1, "XSS")
     .when(F.col("is_traversal") == 1, "PathTraversal")
     .when(F.col("is_tool_scan") == 1, "ToolScan")
     .otherwise("Unknown")
)

# ── 5. Résultats : top patterns par volume ───────────────────────────────────
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

# ── 6. Top 10 IPs malveillantes (suspicious + malicious) ────────────────────
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

# ── 7. Corrélation bytes_transferred ↔ threat_label ─────────────────────────
print("\n>>> BYTES TRANSFERRED BY THREAT LABEL:")
bytes_correlation = (
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
bytes_correlation.show(truncate=False)

# ── 8. Patterns SQLi les plus fréquents (top request paths) ─────────────────
print("\n>>> TOP SQLi REQUEST PATHS:")
top_sqli_paths = (
    df_classified
    .filter(F.col("is_sqli") == 1)
    .groupBy("request_path", "threat_label")
    .count()
    .orderBy(F.col("count").desc())
    .limit(20)
)
top_sqli_paths.show(20, truncate=False)

# ── 9. Sauvegarde des vues batch en Parquet ──────────────────────────────────
OUTPUT_BASE = "hdfs://namenode:9000/data/cybersecurity/batch"
LOCAL_BASE  = "/home/jovyan/work/data/batch_output"

def save(df_to_save, hdfs_path, local_path, name):
    try:
        df_to_save.write.mode("overwrite").parquet(hdfs_path)
        print(f">>> [{name}] saved to HDFS: {hdfs_path}")
    except Exception as e:
        print(f">>> [{name}] HDFS write failed ({e}), saving locally...")
        df_to_save.write.mode("overwrite").parquet(local_path)
        print(f">>> [{name}] saved locally: {local_path}")

save(attack_summary,     f"{OUTPUT_BASE}/attack_patterns",   f"{LOCAL_BASE}/attack_patterns",   "attack_summary")
save(top_ips,            f"{OUTPUT_BASE}/top_malicious_ips", f"{LOCAL_BASE}/top_malicious_ips", "top_ips")
save(bytes_correlation,  f"{OUTPUT_BASE}/bytes_correlation", f"{LOCAL_BASE}/bytes_correlation", "bytes_correlation")

# df_classified with attack flags — useful for HBase ingestion later
save(
    df_classified.filter(F.col("attack_type") != "Unknown"),
    f"{OUTPUT_BASE}/classified_attacks",
    f"{LOCAL_BASE}/classified_attacks",
    "classified_attacks"
)

print("\n>>> Attack pattern detection complete!")
spark.stop()