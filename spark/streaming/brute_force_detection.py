"""
brute_force_detection.py
RAPID — Speed Layer — Sprint 2
Detection brute-force: 5+ blocked / 1 min / IP → Cassandra

Cassandra schema (cybersecurity.realtime_threats):
    ip_source    text
    last_seen    timestamp
    attack_types text        ← plain text, NOT list
    threat_score int         ← int, NOT float
    PRIMARY KEY (ip_source, last_seen)
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, window, count, from_json, to_timestamp,
    current_timestamp, lit
)
from pyspark.sql.types import StructType, StructField, StringType, LongType

KAFKA_HOST     = "100.73.216.115:9092"
CASSANDRA_HOST = "100.97.208.110"
KEYSPACE       = "cybersecurity"
TABLE          = "realtime_threats"

schema = StructType([
    StructField("timestamp",         StringType()),
    StructField("source_ip",         StringType()),
    StructField("dest_ip",           StringType()),
    StructField("protocol",          StringType()),
    StructField("action",            StringType()),
    StructField("threat_label",      StringType()),
    StructField("log_type",          StringType()),
    StructField("bytes_transferred", LongType()),
    StructField("user_agent",        StringType()),
    StructField("request_path",      StringType()),
])

spark = SparkSession.builder \
    .appName("RAPID-BruteForceDetection") \
    .config("spark.cassandra.connection.host", CASSANDRA_HOST) \
    .config("spark.cassandra.connection.port", "9042") \
    .config("spark.sql.shuffle.partitions", "4") \
    .config("spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
        "com.datastax.spark:spark-cassandra-connector_2.12:3.5.0") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("=" * 60)
print("  RAPID - Brute Force Detection")
print(f"  Kafka     : {KAFKA_HOST}")
print(f"  Cassandra : {CASSANDRA_HOST} / {KEYSPACE}.{TABLE}")
print("=" * 60)

raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_HOST) \
    .option("subscribe", "cybersecurity-logs") \
    .option("startingOffsets", "latest") \
    .option("maxOffsetsPerTrigger", 1000) \
    .option("failOnDataLoss", "false") \
    .option("kafka.request.timeout.ms", "120000") \
    .option("kafka.session.timeout.ms", "60000") \
    .load()

logs = raw.selectExpr("CAST(value AS STRING) as json_str") \
    .select(from_json(col("json_str"), schema).alias("data")) \
    .select("data.*") \
    .withColumn("event_time", to_timestamp(col("timestamp"))) \
    .withWatermark("event_time", "2 minutes")

brute_force = logs \
    .filter(col("action") == "blocked") \
    .groupBy(
        col("source_ip"),
        window(col("event_time"), "1 minute")
    ) \
    .agg(count("*").alias("failed_attempts")) \
    .filter(col("failed_attempts") >= 5) \
    .select(
        col("source_ip"),
        col("failed_attempts"),
        col("window.end").alias("last_seen"),
        # attack_types = text (not list)
        lit("brute_force").alias("attack_types"),
        # threat_score = int (not float)
        (col("failed_attempts") * 10).cast("int").alias("threat_score")
    )

def write_to_cassandra(batch_df, batch_id):
    count_val = batch_df.count()
    if count_val == 0:
        print(f">>> Batch {batch_id}: no brute-force detected")
        return

    print(f"\n>>> BRUTE FORCE — Batch {batch_id}: {count_val} IPs flagged")
    batch_df.show(truncate=False)

    cassandra_df = batch_df.select(
        col("source_ip").alias("ip_source"),  # text PRIMARY KEY
        col("last_seen"),                      # timestamp CLUSTERING KEY
        col("attack_types"),                   # text
        col("threat_score"),                   # int
    )

    try:
        cassandra_df.write \
            .format("org.apache.spark.sql.cassandra") \
            .options(table=TABLE, keyspace=KEYSPACE) \
            .mode("append") \
            .save()
        print(f">>> {count_val} records written to {KEYSPACE}.{TABLE}")
    except Exception as e:
        print(f">>> Cassandra write error: {e}")

query = brute_force.writeStream \
    .outputMode("update") \
    .foreachBatch(write_to_cassandra) \
    .option("checkpointLocation", "/home/jovyan/work/streaming/chkpt_brute") \
    .trigger(processingTime="30 seconds") \
    .start()

print(">>> Stream started — waiting for events...")
print(">>> Threshold: 5+ blocked / 1 min / IP")
print(">>> Ctrl+C to stop\n")
query.awaitTermination()