#!/usr/bin/env python3
"""
Fast sample test - reads only 10000 messages to calculate volumes
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, sum as _sum, count, avg, max as _max, min as _min, desc
from pyspark.sql.types import StructType, StringType, LongType

KAFKA_BROKER = "100.73.216.115:9092"
TOPIC        = "cybersecurity-logs"
SAMPLE_SIZE  = 10000  # only read 10000 messages

spark = SparkSession.builder \
    .appName("VolumeSampleTest") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

schema = StructType() \
    .add("source_ip",         StringType()) \
    .add("dest_ip",           StringType()) \
    .add("bytes_transferred", StringType()) \
    .add("action",            StringType()) \
    .add("threat_label",      StringType()) \
    .add("log_type",          StringType()) \
    .add("protocol",          StringType()) \
    .add("request_path",      StringType()) \
    .add("user_agent",        StringType()) \
    .add("timestamp",         StringType())

print(f"Reading {SAMPLE_SIZE} sample messages from Kafka...")

df = spark.read \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER) \
    .option("subscribe", TOPIC) \
    .option("startingOffsets", "earliest") \
    .option("endingOffsets", f'{{"cybersecurity-logs":{{"0":{SAMPLE_SIZE//3},"1":{SAMPLE_SIZE//3},"2":{SAMPLE_SIZE//3}}}}}') \
    .load()

parsed = df.select(
    from_json(col("value").cast("string"), schema).alias("d")
).select("d.*") \
 .withColumn("bytes_transferred", col("bytes_transferred").cast(LongType()))

total = parsed.count()
print(f"\n{'='*50}")
print(f"SAMPLE SIZE : {total} messages")
print(f"TOTAL IN KAFKA : ~6,000,000 messages")
print(f"{'='*50}")

# Per message stats
print("\n=== BYTES PER MESSAGE ===")
stats = parsed.select(
    _sum("bytes_transferred").alias("total_bytes"),
    avg("bytes_transferred").alias("avg_bytes"),
    _max("bytes_transferred").alias("max_bytes"),
    _min("bytes_transferred").alias("min_bytes")
).collect()[0]

avg_bytes  = stats["avg_bytes"] or 0
max_bytes  = stats["max_bytes"] or 0
min_bytes  = stats["min_bytes"] or 0
total_bytes = stats["total_bytes"] or 0

print(f"  avg per message : {avg_bytes/1024:.2f} KB")
print(f"  max per message : {max_bytes/1024:.2f} KB")
print(f"  min per message : {min_bytes/1024:.2f} KB")
print(f"  total sample    : {total_bytes/1048576:.2f} MB")

# Unique IPs in sample
unique_ips = parsed.select("source_ip").distinct().count()
print(f"\n=== IP STATS ===")
print(f"  unique IPs in sample : {unique_ips}")
print(f"  avg msgs per IP      : {total/unique_ips:.1f}")
print(f"  avg bytes per IP     : {total_bytes/unique_ips/1024:.2f} KB")

# Top 10 IPs
print("\n=== TOP 10 IPs BY BYTES (in sample) ===")
top_ips = parsed.groupBy("source_ip") \
                .agg(
                    _sum("bytes_transferred").alias("total_bytes"),
                    count("*").alias("msg_count")
                ) \
                .orderBy(desc("total_bytes")) \
                .limit(10) \
                .collect()

for row in top_ips:
    kb = row["total_bytes"] / 1024
    mb = row["total_bytes"] / 1048576
    print(f"  {row['source_ip']:<20} | {row['msg_count']:>5} msgs | {kb:>10.1f} KB | {mb:>6.2f} MB")

# Calculate recommendation
print(f"\n{'='*50}")
print(f"WINDOW & THRESHOLD RECOMMENDATION")
print(f"{'='*50}")

# With maxOffsetsPerTrigger=50000 per 10 seconds
msgs_per_10s    = 50000
bytes_per_10s   = msgs_per_10s * avg_bytes
bytes_per_ip_10s = bytes_per_10s / unique_ips

print(f"\nWith maxOffsetsPerTrigger=50000 per 10 seconds:")
print(f"  total bytes per 10s          : {bytes_per_10s/1048576:.1f} MB")
print(f"  avg bytes per IP per 10s     : {bytes_per_ip_10s/1024:.1f} KB")
print(f"  top IP bytes per 10s (est.)  : {top_ips[0]['total_bytes']/total*msgs_per_10s/1024:.1f} KB")

top_ip_bytes_per_10s = top_ips[0]["total_bytes"] / total * msgs_per_10s

print(f"\nRECOMMENDED SETTINGS:")
print(f"  THRESHOLD_BYTES = {int(top_ip_bytes_per_10s * 0.5 / 1024)} * 1024  # 50% of top IP rate")
print(f"  WINDOW_SECONDS  = 10")
print(f"\n  This means: alert if any IP sends > {top_ip_bytes_per_10s*0.5/1024:.0f} KB in 10 seconds")
