"""
attack_signature_detection.py
RAPID — Speed Layer — Sprint 2
Detection signatures: sqlmap, nikto, OR 1=1, XSS → Cassandra

Cassandra schema (cybersecurity.signature_alerts):
    source_ip    text
    timestamp    text        ← stored as text string
    reason       text        ← attack type description
    request_path text
    threat_label text
    user_agent   text
    PRIMARY KEY (source_ip, timestamp)
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, to_timestamp, when, lit,
    regexp_extract, date_format
)
from pyspark.sql.types import StructType, StructField, StringType, LongType
import os

KAFKA_HOST     = "100.73.216.115:9092"
CASSANDRA_HOST = "100.97.208.110"
KEYSPACE       = "cybersecurity"
TABLE          = "signature_alerts"
STARTING_OFFSETS = os.getenv("RAPID_STARTING_OFFSETS", "earliest")
CHECKPOINT_LOCATION = os.getenv(
    "RAPID_SIGNATURES_CHECKPOINT",
    "/tmp/rapid_streaming/signatures"
)

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
    .appName("RAPID-AttackSignatureDetection") \
    .config("spark.cassandra.connection.host", CASSANDRA_HOST) \
    .config("spark.cassandra.connection.port", "9042") \
    .config("spark.sql.shuffle.partitions", "4") \
    .config("spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
        "com.datastax.spark:spark-cassandra-connector_2.12:3.5.0") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("=" * 60)
print("  RAPID - Attack Signature Detection")
print(f"  Kafka     : {KAFKA_HOST}")
print(f"  Cassandra : {CASSANDRA_HOST} / {KEYSPACE}.{TABLE}")
print("=" * 60)

SQLI_PATTERN  = r"(?i)(union\s+select|or\s+1=1|'\s+or\s+'|drop\s+table|insert\s+into|--|xp_cmdshell)"
XSS_PATTERN   = r"(?i)(<script|javascript:|onerror=|onload=|alert\(|document\.cookie)"
TOOLS_PATTERN = r"(?i)(sqlmap|nikto|nmap|masscan|hydra|metasploit)"

raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_HOST) \
    .option("subscribe", "cybersecurity-logs") \
    .option("startingOffsets", STARTING_OFFSETS) \
    .option("maxOffsetsPerTrigger", 1000) \
    .option("failOnDataLoss", "false") \
    .option("kafka.request.timeout.ms", "120000") \
    .option("kafka.session.timeout.ms", "60000") \
    .load()

logs = raw.selectExpr("CAST(value AS STRING) as json_str") \
    .select(from_json(col("json_str"), schema).alias("data")) \
    .select("data.*") \
    .withColumn("event_time", to_timestamp(col("timestamp")))

attacks = logs \
    .withColumn("sqli",  regexp_extract(col("request_path"), SQLI_PATTERN, 0)) \
    .withColumn("xss",   regexp_extract(col("request_path"), XSS_PATTERN, 0)) \
    .withColumn("tools", regexp_extract(col("user_agent"),   TOOLS_PATTERN, 0)) \
    .withColumn("reason",
        when(col("sqli")  != "", lit("SQLi injection detected"))
       .when(col("xss")   != "", lit("XSS attack detected"))
       .when(col("tools") != "", lit("Scan tool detected"))
       .otherwise(lit("clean"))) \
    .filter(col("reason") != "clean") \
    .select(
        col("source_ip"),
        # timestamp stored as text in Cassandra — format as string
        date_format(col("event_time"), "yyyy-MM-dd HH:mm:ss").alias("timestamp"),
        col("reason"),
        col("request_path"),
        col("threat_label"),
        col("user_agent"),
    )

def write_to_cassandra(batch_df, batch_id):
    count_val = batch_df.count()
    if count_val == 0:
        print(f">>> Batch {batch_id}: no signatures detected")
        return

    print(f"\n>>> SIGNATURES — Batch {batch_id}: {count_val} attacks flagged")
    batch_df.show(truncate=False)

    # Columns match exactly: source_ip, timestamp(text), reason, request_path, threat_label, user_agent
    try:
        batch_df.write \
            .format("org.apache.spark.sql.cassandra") \
            .options(table=TABLE, keyspace=KEYSPACE) \
            .mode("append") \
            .save()
        print(f">>> {count_val} records written to {KEYSPACE}.{TABLE}")
    except Exception as e:
        print(f">>> Cassandra write error: {e}")

query = attacks.writeStream \
    .outputMode("append") \
    .foreachBatch(write_to_cassandra) \
    .option("checkpointLocation", CHECKPOINT_LOCATION) \
    .trigger(processingTime="15 seconds") \
    .start()

print(">>> Stream started — detecting SQLi, XSS, ScanTools...")
print(">>> Ctrl+C to stop\n")
query.awaitTermination()
