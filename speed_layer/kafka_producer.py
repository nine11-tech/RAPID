# speed_layer/kafka_producer.py
from kafka import KafkaProducer
import pandas as pd, time, json, os

KAFKA_BROKER = os.getenv("100.73.216.115", "100.64.0.1") + ":9092"
TOPIC = "cybersecurity-logs"
CSV_PATH = "/data/cybersecurity_threat_detection_logs.csv"

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

df = pd.read_csv(CSV_PATH)
for _, row in df.iterrows():
    producer.send(TOPIC, row.to_dict())
    print(f"[SENT] {row.get('src_ip', '?')}")
    time.sleep(0.1)  # simule un flux temps réel

producer.flush()
print("Done.")