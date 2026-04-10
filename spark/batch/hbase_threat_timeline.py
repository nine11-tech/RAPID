from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, date_trunc, to_timestamp

spark = SparkSession.builder \
    .appName("RAPID - HBase Threat Timeline") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# ── Lire le CSV depuis HDFS
print(">>> Loading dataset...")
df = spark.read.csv(
    "hdfs://namenode:9000/data/cybersecurity/logs/year=2024/month=10/day=15/cybersecurity_threat_detection_logs.csv",
    header=True,
    inferSchema=True
)

# ── Parser le timestamp
df = df.withColumn("timestamp", to_timestamp(col("timestamp")))

# ── Agrégation par heure + threat_label
print(">>> Aggregating by hour...")
timeline_hourly = df.groupBy(
    date_trunc("hour", col("timestamp")).alias("heure"),
    col("threat_label")
).agg(count("*").alias("event_count")) \
 .orderBy("heure", "threat_label")

print("\n=== THREAT TIMELINE (par heure) ===")
timeline_hourly.show(20, truncate=False)

# ── Agrégation par jour aussi
timeline_daily = df.groupBy(
    date_trunc("day", col("timestamp")).alias("jour"),
    col("threat_label")
).agg(count("*").alias("event_count")) \
 .orderBy("jour", "threat_label")

print("\n=== THREAT TIMELINE (par jour) ===")
timeline_daily.show(20, truncate=False)

# ── Sauvegarde Parquet dans HDFS (intermédiaire avant HBase)
timeline_hourly.write.mode("overwrite") \
    .parquet("hdfs://namenode:9000/data/cybersecurity/batch/threat_timeline_hourly")

timeline_daily.write.mode("overwrite") \
    .parquet("hdfs://namenode:9000/data/cybersecurity/batch/threat_timeline_daily")

print(">>> Parquet saved to HDFS!")

# ── Écriture dans HBase via foreachPartition
print(">>> Writing to HBase...")

def write_to_hbase(rows):
    import happybase
    connection = happybase.Connection('hbase', port=16000)
    table = connection.table('threat_timeline')
    for row in rows:
        # row_key = heure|threat_label
        row_key = f"{str(row['heure'])}|{row['threat_label']}"
        table.put(row_key.encode(), {
            b'cf:heure': str(row['heure']).encode(),
            b'cf:threat_label': str(row['threat_label']).encode(),
            b'cf:event_count': str(row['event_count']).encode()
        })
    connection.close()

timeline_hourly.foreachPartition(write_to_hbase)
print(">>> HBase threat_timeline populated!")
spark.stop()