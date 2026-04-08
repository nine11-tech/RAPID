from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("RAPID - Load timeline to HBase shell") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Lire le Parquet déjà sauvegardé
df = spark.read.parquet(
    "hdfs://namenode:9000/data/cybersecurity/batch/threat_timeline_hourly"
)

# Générer les commandes HBase shell
rows = df.filter(df.heure.isNotNull()).collect()

hbase_commands = []
for row in rows:
    row_key = f"{str(row['heure'])}|{str(row['threat_label'])}"
    cmd = f"put 'threat_timeline', '{row_key}', 'cf:heure', '{str(row['heure'])}'"
    cmd2 = f"put 'threat_timeline', '{row_key}', 'cf:threat_label', '{str(row['threat_label'])}'"
    cmd3 = f"put 'threat_timeline', '{row_key}', 'cf:event_count', '{str(row['event_count'])}'"
    hbase_commands.extend([cmd, cmd2, cmd3])

# Sauvegarder les commandes dans un fichier
with open("/tmp/hbase_inserts.txt", "w") as f:
    f.write("\n".join(hbase_commands))
    f.write("\nexit\n")

print(f">>> Generated {len(rows)} rows = {len(hbase_commands)} HBase commands")
print(">>> File saved: /tmp/hbase_inserts.txt")
spark.stop()