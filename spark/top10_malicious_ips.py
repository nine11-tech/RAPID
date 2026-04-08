from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, desc

# ─── Init Spark ───────────────────────────────────────────
spark = SparkSession.builder \
    .appName("RAPID - Top 10 Malicious IPs") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# ─── Lecture depuis HDFS ──────────────────────────────────
print(">>> Loading dataset from HDFS...")
df = spark.read.csv(
    "hdfs://namenode:9000/data/cybersecurity/logs/year=2024/month=10/day=15/cybersecurity_threat_detection_logs.csv",
    header=True,
    inferSchema=True
)

print(f">>> Total rows: {df.count()}")
df.printSchema()

# ─── Top 10 IPs malveillantes ─────────────────────────────
print("\n>>> Computing Top 10 Malicious IPs...")
top10 = df.filter(col("threat_label").isin("suspicious", "malicious")) \
          .groupBy("source_ip") \
          .agg(count("*").alias("threat_count")) \
          .orderBy(desc("threat_count")) \
          .limit(10)

top10.show(truncate=False)

# ─── Sauvegarde en Parquet ────────────────────────────────
top10.write.mode("overwrite") \
    .parquet("hdfs://namenode:9000/data/cybersecurity/batch/top10_ips")

print(">>> Results saved to HDFS: /data/cybersecurity/batch/top10_ips")
spark.stop()