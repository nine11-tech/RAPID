from kafka import KafkaProducer
import json, os, csv, urllib.request, time

ANASS_IP = os.getenv("ANASS_IP", "100.73.216.115")
KAFKA_BROKER = f"{ANASS_IP}:9092"
HDFS_WEB = f"http://{ANASS_IP}:9870"
TOPIC = "cybersecurity-logs"

STATE_FILE = os.getenv("STATE_FILE", "/tmp/kafka_producer_state.json")
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "5000"))

def load_state():
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE, "r") as f:
            return json.load(f)
    return {"month": 1, "line": 0, "total": 0}

def save_state(month, line, total):
    with open(STATE_FILE, "w") as f:
        json.dump({"month": month, "line": line, "total": total}, f)

state = load_state()
start_month = int(state["month"])
start_line = int(state["line"])
total = int(state["total"])

print(f"[INFO] Kafka: {KAFKA_BROKER}")
print(f"[INFO] Resume from month={start_month:02d}, line={start_line}, total={total}")

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    key_serializer=lambda k: str(k).encode("utf-8"),
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    linger_ms=50,
    batch_size=32768
)

try:
    for month in range(start_month, 13):
        month_str = f"{month:02d}"
        url = f"{HDFS_WEB}/webhdfs/v1/logs/year=2024/month={month_str}/data.csv?op=OPEN&user.name=root"

        print(f"\n[INFO] Reading month={month_str} from line={start_line}")

        with urllib.request.urlopen(url) as resp:
            text = resp.read().decode("utf-8").splitlines()

        reader = csv.DictReader(text)
        sent_this_month = 0

        for i, row in enumerate(reader):
            if month == start_month and i < start_line:
                continue

            key = row.get("source_ip", "unknown")
            producer.send(TOPIC, key=key, value=row)

            total += 1
            sent_this_month += 1

            if total % BATCH_SIZE == 0:
                producer.flush()
                save_state(month, i + 1, total)
                print(f"[SENT] total={total} | month={month_str} | next_line={i+1}")
                time.sleep(0.1)

        producer.flush()
        print(f"[OK] month={month_str} done | sent={sent_this_month}")

        save_state(month + 1, 0, total)
        start_line = 0

    print(f"\n✅ DONE all months — total sent={total}")
    if os.path.exists(STATE_FILE):
        os.remove(STATE_FILE)

except KeyboardInterrupt:
    producer.flush()
    print("\n[STOPPED] Progress saved. Re-run same script to continue.")

except Exception as e:
    producer.flush()
    print(f"\n[ERROR] {e}")
    print("[INFO] Progress saved. Fix issue and re-run.")

finally:
    producer.close()