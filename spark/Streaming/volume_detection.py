#!/usr/bin/env python3
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, sum as _sum
from pyspark.sql.types import StructType, StringType, LongType
from datetime import datetime
from collections import defaultdict
import time

KAFKA_BROKER    = "100.73.216.115:9092"
CASSANDRA_HOST  = "100.97.208.110"
TOPIC           = "cybersecurity-logs"
THRESHOLD_BYTES = 2720 * 1024   # 2720 KB — 50% of top IP rate (5440 KB/10s estimated from 6M-message sample)
WINDOW_SECONDS  = 10

spark = SparkSession.builder \
    .appName("Task5_VolumeDetection") \
    .config("spark.cassandra.connection.host", CASSANDRA_HOST) \
    .config("spark.cassandra.connection.port", "9042") \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")

print(f"Kafka      : {KAFKA_BROKER}")
print(f"Cassandra  : {CASSANDRA_HOST}")
print(f"Threshold  : {THRESHOLD_BYTES/1024:.0f} KB per {WINDOW_SECONDS} seconds")

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

raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER) \
    .option("subscribe", TOPIC) \
    .option("startingOffsets", "latest") \
    .option("maxOffsetsPerTrigger", 50000) \
    .option("failOnDataLoss", "false") \
    .load()

parsed = raw.select(
    from_json(col("value").cast("string"), schema).alias("d")
).select("d.*") \
 .withColumn("bytes_transferred", col("bytes_transferred").cast(LongType()))

window_state = defaultdict(list)

def write_volume_alerts(batch_df, batch_id):
    total_rows = batch_df.count()
    print(f"[Batch {batch_id}] Received {total_rows} rows")
    if total_rows == 0:
        return

    now = time.time()
    window_start_time = now - WINDOW_SECONDS

    grouped = batch_df.groupBy("source_ip") \
                      .agg(_sum("bytes_transferred").alias("batch_bytes"))

    for row in grouped.collect():
        ip = row["source_ip"]
        if ip and row["batch_bytes"]:
            window_state[ip].append((now, row["batch_bytes"]))

    for ip in list(window_state.keys()):
        window_state[ip] = [(t, b) for t, b in window_state[ip] if t >= window_start_time]
        if not window_state[ip]:
            del window_state[ip]

    alerts = []
    for ip, entries in window_state.items():
        total = sum(b for _, b in entries)
        print(f"  {ip} -> {total/1024:.1f} KB")
        if total > THRESHOLD_BYTES:
            alerts.append((ip, total))

    print(f"[Batch {batch_id}] {len(alerts)} IPs exceeded {THRESHOLD_BYTES/1024:.0f} KB")
    if not alerts:
        return

    now_iso = datetime.now().isoformat()
    window_start_iso = datetime.fromtimestamp(window_start_time).isoformat()

    alert_rows = []
    for ip, total in alerts:
        print(f"[ALERT] {ip} | {total/1024:.1f} KB")
        alert_rows.append((
            ip,
            window_start_iso,
            now_iso,
            total,
            THRESHOLD_BYTES,
            "Volume exceeded threshold",
            now_iso
        ))

    alert_df = spark.createDataFrame(alert_rows, [
        "source_ip",
        "window_start",
        "window_end",
        "total_bytes",
        "threshold",
        "reason",
        "created_at"
    ])

    alert_df.write \
        .format("org.apache.spark.sql.cassandra") \
        .option("keyspace", "cybersecurity") \
        .option("table", "volume_alerts") \
        .mode("append") \
        .save()

    print(f"[Batch {batch_id}] Written {len(alert_rows)} alerts to Cassandra")

if __name__ == "__main__":
    print("\nStream running - detecting volume anomalies...")
    query = parsed.writeStream \
        .foreachBatch(write_volume_alerts) \
        .outputMode("append") \
        .trigger(processingTime="10 seconds") \
        .start()

    query.awaitTermination()
