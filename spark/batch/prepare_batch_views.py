from pyspark.sql import SparkSession
from pyspark.sql.functions import count, col
import os

ANASS_IP = os.getenv("ANASS_IP", "100.73.216.115")
CHAWI_IP = os.getenv("CHAWI_IP", "100.97.208.110")

spark = SparkSession.builder \
    .appName("PrepareViews") \
    .getOrCreate()

HDFS = f"hdfs://{ANASS_IP}:9000/data/cybersecurity/batch"

# Vue 1 — Top 10 IPs (depuis top_malicious_ips déjà calculé)
top10 = spark.read.parquet(f"{HDFS}/top_malicious_ips")
top10.show(10)

# Vue 2 — Timeline des attaques (depuis attack_patterns)
timeline = spark.read.parquet(f"{HDFS}/attack_patterns")
timeline.show(10)

# Sauvegarder en parquet pour l'API d'Anass
top10.limit(10).write.mode("overwrite") \
    .parquet(f"{HDFS}/views/top10")

timeline.groupBy("attack_type") \
    .agg(count("*").alias("nb_events")) \
    .write.mode("overwrite") \
    .parquet(f"{HDFS}/views/timeline")

print("=== Views pretes pour l'API Anass ===")
spark.stop()