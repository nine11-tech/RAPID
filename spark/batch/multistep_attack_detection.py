# spark/batch/multistep_attack_detection.py
import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

ANASS_IP = os.getenv("ANASS_IP", "100.73.216.115")
CHAWI_IP = os.getenv("CHAWI_IP", "100.97.208.110")

spark = SparkSession.builder \
    .appName("MultistepAttackDetection") \
    .config("spark.sql.shuffle.partitions", "8") \
    .config("spark.hadoop.dfs.client.use.datanode.hostname", "true") \
    .config("spark.hadoop.dfs.datanode.use.datanode.hostname", "true") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

HDFS = f"hdfs://{ANASS_IP}:9000/data/cybersecurity/batch"

print("=== Chargement classified_attacks ===")
df = spark.read.parquet(f"{HDFS}/classified_attacks")
df.printSchema()

# ── Étape 1 : IPs qui ont fait du ToolScan (reconnaissance)
ips_recon = df.filter(F.col("attack_type") == "ToolScan") \
    .select("source_ip").distinct()

# ── Étape 2 : IPs qui ont fait du SQLi (exploitation)
ips_sqli = df.filter(F.col("attack_type") == "SQLi") \
    .select("source_ip").distinct()

# ── Étape 3 : IPs qui ont fait du PathTraversal (persistance)
ips_traversal = df.filter(F.col("attack_type") == "PathTraversal") \
    .select("source_ip").distinct()

# ── Étape 4 : Intersection — IPs qui ont fait les 3 étapes
print("\n=== IPs multi-étapes (ToolScan + SQLi + PathTraversal) ===")
multistep_ips = ips_recon \
    .join(ips_sqli, "source_ip") \
    .join(ips_traversal, "source_ip")

print(f"Total IPs multi-étapes: {multistep_ips.count()}")
multistep_ips.show(20, truncate=False)

# ── Étape 5 : Enrichir avec les stats complètes par IP
stats = df.groupBy("source_ip").agg(
    F.count("*").alias("total_events"),
    F.collect_set("attack_type").alias("attack_types"),
    F.sum("is_sqli").alias("sqli_hits"),
    F.sum("is_traversal").alias("traversal_hits"),
    F.sum("is_tool_scan").alias("tool_hits"),
    F.avg("bytes_transferred").alias("avg_bytes"),
    F.sum(F.when(F.col("threat_label") == "malicious", 1).otherwise(0)).alias("malicious_count")
)

result = multistep_ips.join(stats, "source_ip") \
    .withColumn("attack_chain", F.lit("ToolScan -> SQLi -> PathTraversal")) \
    .withColumn("ordered_steps", F.array(F.lit("ToolScan"), F.lit("SQLi"), F.lit("PathTraversal"))) \
    .withColumn("step_count", F.lit(3)) \
    .withColumn(
        "risk_level",
        F.when(F.col("malicious_count") > 100, "CRITICAL")
         .when(F.col("malicious_count") > 50, "HIGH")
         .otherwise("MEDIUM")
    ) \
    .orderBy(F.col("total_events").desc())

# Cache result car utilisé deux fois (parquet + hbase)
result.cache()

print("\n=== Résultat final enrichi ===")
result.show(20, truncate=False)

# ── Étape 6 : Sauvegarder dans HDFS
print("\n=== Sauvegarde views/multistep_attacks ===")
result.write.mode("overwrite") \
    .parquet(f"{HDFS}/views/multistep_attacks")

# ── Étape 7 : Écriture HBase → Chawi
print(f"\n=== Écriture HBase @ {CHAWI_IP}:9090 ===")

# Installer happybase si absent
import subprocess, sys
subprocess.run([sys.executable, "-m", "pip", "install", "happybase", "-q"], check=True)

def write_multistep_hbase(rows):
    import happybase
    try:
        conn = happybase.Connection(CHAWI_IP, port=9090)
        tbl = conn.table('multistep_attacks')
        for row in rows:
            rk = str(row['source_ip'])
            tbl.put(rk.encode(), {
                b'cf:total_events':    str(row['total_events']).encode(),
                b'cf:attack_types':    str(row['attack_types']).encode(),
                b'cf:attack_chain':    str(row['attack_chain']).encode(),
                b'cf:ordered_steps':   str(row['ordered_steps']).encode(),
                b'cf:step_count':      str(row['step_count']).encode(),
                b'cf:sqli_hits':       str(row['sqli_hits']).encode(),
                b'cf:traversal_hits':  str(row['traversal_hits']).encode(),
                b'cf:tool_hits':       str(row['tool_hits']).encode(),
                b'cf:avg_bytes':       str(row['avg_bytes']).encode(),
                b'cf:malicious_count': str(row['malicious_count']).encode(),
                b'cf:risk_level':      str(row['risk_level']).encode(),
            })
        conn.close()
    except Exception as e:
        print(f"[WARN] HBase write failed: {e}")

result.foreachPartition(write_multistep_hbase)
print("=== HBase multistep_attacks done ===")

print("\n=== Done: multistep_attacks complete ===")
spark.stop()
