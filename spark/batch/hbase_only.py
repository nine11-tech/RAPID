import os
import happybase
from pyspark.sql import SparkSession

ANASS_IP = os.getenv("ANASS_IP", "100.73.216.115")
CHAWI_IP = os.getenv("CHAWI_IP", "100.97.208.110")

NAMENODE = f"hdfs://{ANASS_IP}:9000"
HDFS_OUT = f"{NAMENODE}/data/cybersecurity/batch"

spark = SparkSession.builder.appName("RAPID-HBaseOnly").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

attack_summary = spark.read.parquet(f"{HDFS_OUT}/attack_patterns")
top_ips        = spark.read.parquet(f"{HDFS_OUT}/top_malicious_ips")

CHAWI = CHAWI_IP

print(f">>> Writing attack_patterns to HBase @ {CHAWI}:9090 ...")

def write_attack_patterns_hbase(rows):
    import happybase
    conn = happybase.Connection(CHAWI, port=9090)
    tbl  = conn.table("attack_patterns")
    for row in rows:
        rk = f"{row['attack_type']}|{row['threat_label']}"
        tbl.put(rk.encode(), {
            b"cf:attack_type":  str(row["attack_type"]).encode(),
            b"cf:threat_label": str(row["threat_label"]).encode(),
            b"cf:occurrences":  str(row["occurrences"]).encode(),
            b"cf:avg_bytes":    str(row["avg_bytes"]).encode(),
            b"cf:total_bytes":  str(row["total_bytes"]).encode(),
        })
    conn.close()

attack_summary.foreachPartition(write_attack_patterns_hbase)
print(">>> attack_patterns HBase done!")

print(f">>> Writing ip_reputation to HBase @ {CHAWI}:9090 ...")
top_ips_rows = top_ips.collect()
conn = happybase.Connection(CHAWI, port=9090)
tbl  = conn.table("ip_reputation")
for row in top_ips_rows:
    rk = str(row["source_ip"])
    tbl.put(rk.encode(), {
        b"cf:threat_count":   str(row["threat_count"]).encode(),
        b"cf:sqli_hits":      str(row["sqli_hits"]).encode(),
        b"cf:xss_hits":       str(row["xss_hits"]).encode(),
        b"cf:traversal_hits": str(row["traversal_hits"]).encode(),
        b"cf:tool_hits":      str(row["tool_hits"]).encode(),
        b"cf:avg_bytes":      str(row["avg_bytes"]).encode(),
    })
conn.close()
print(">>> ip_reputation HBase done!")

print(">>> HBase write COMPLETE!")
spark.stop()