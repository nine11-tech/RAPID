from pyspark.sql import SparkSession
from pyspark.sql.functions import sum as _sum

spark = SparkSession.builder.appName("show").master("local[*]").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# ── PAR HEURE
df_hourly = spark.read.parquet("hdfs://namenode:9000/data/cybersecurity/batch/threat_timeline_hourly")
df_hourly = df_hourly.filter(df_hourly.heure.isNotNull()).orderBy("heure", "threat_label")

print("\n" + "="*60)
print("       THREAT TIMELINE — AGREGATION HORAIRE")
print("="*60)
df_hourly.show(30, truncate=False)

# ── PAR JOUR
df_daily = spark.read.parquet("hdfs://namenode:9000/data/cybersecurity/batch/threat_timeline_daily")
df_daily = df_daily.filter(df_daily.jour.isNotNull()).orderBy("jour", "threat_label")

print("\n" + "="*60)
print("       THREAT TIMELINE — AGREGATION JOURNALIERE")
print("="*60)
df_daily.show(50, truncate=False)

# ── RESUME GLOBAL
print("\n" + "="*60)
print("       RESUME GLOBAL PAR THREAT LABEL")
print("="*60)
df_daily.groupBy("threat_label") \
        .agg(_sum("event_count").alias("total_events")) \
        .orderBy("threat_label") \
        .show(truncate=False)

spark.stop()
