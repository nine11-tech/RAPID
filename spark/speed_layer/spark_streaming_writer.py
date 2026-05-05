# speed_layer/spark_streaming_writer.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import *
import os

ANASS_IP       = os.getenv("ANASS_IP", "100.73.216.115")
CHAWI_IP       = os.getenv("CHAWI_IP", "100.97.208.110")
KAFKA_BROKER   = f"{ANASS_IP}:9092"
CASSANDRA_HOST = CHAWI_IP
TOPIC          = "cybersecurity-logs"

print(f"[INFO] Kafka     : {KAFKA_BROKER}")
print(f"[INFO] Cassandra : {CASSANDRA_HOST}:9042")

schema = StructType([
    StructField("timestamp",         StringType()),
    StructField("source_ip",         StringType()),
    StructField("dest_ip",           StringType()),
    StructField("protocol",          StringType()),
    StructField("action",            StringType()),
    StructField("threat_label",      StringType()),
    StructField("log_type",          StringType()),
    StructField("bytes_transferred", StringType()),
    StructField("user_agent",        StringType()),
    StructField("request_path",      StringType()),
])

spark = SparkSession.builder \
    .appName("Kafka_to_Cassandra_Stream") \
    .config("spark.cassandra.connection.host", CASSANDRA_HOST) \
    .config("spark.cassandra.connection.port", "9042") \
    .config("spark.cassandra.connection.timeout_ms", "60000") \
    .config("spark.cassandra.read.timeout_ms", "120000") \
    .config("spark.network.timeout", "800s") \
    .config("spark.executor.heartbeatInterval", "60s") \
    .config("spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
            "com.datastax.spark:spark-cassandra-connector_2.12:3.5.1") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

raw_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER) \
    .option("subscribe", TOPIC) \
    .option("startingOffsets", "earliest") \
    .option("failOnDataLoss", "false") \
    .option("kafka.request.timeout.ms", "600000") \
    .option("kafka.session.timeout.ms", "600000") \
    .option("kafka.fetch.max.wait.ms", "30000") \
    .option("kafka.max.partition.fetch.bytes", "52428800") \
    .load()

parsed_df = raw_df \
    .selectExpr("CAST(value AS STRING) as json_str") \
    .select(from_json(col("json_str"), schema).alias("data")) \
    .select("data.*")

def write_to_cassandra(batch_df, batch_id):
    count = batch_df.count()
    print(f"[BATCH {batch_id}] {count} rows → Cassandra")
    if count > 0:
        batch_df.write \
            .format("org.apache.spark.sql.cassandra") \
            .mode("append") \
            .options(table="logs", keyspace="cybersecurity") \
            .save()
        print(f"[BATCH {batch_id}] ✅ OK")

query = parsed_df.writeStream \
    .foreachBatch(write_to_cassandra) \
    .option("checkpointLocation", "/tmp/spark_checkpoint_kafka_cassandra") \
    .trigger(processingTime="60 seconds") \
    .start()

print("✅ Streaming started — Kafka → Cassandra")
query.awaitTermination()