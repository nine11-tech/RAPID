#!/usr/bin/env bash

API_BASE="${API_BASE:-http://100.73.216.115:5000}"
INTERVAL="${INTERVAL:-5}"
TMP="/tmp/rapid_live_monitor"

mkdir -p "$TMP"

while true; do
  clear
  echo "============================================================"
  echo "RAPID LIVE MONITOR"
  echo "API: $API_BASE"
  date
  echo "============================================================"

  echo
  echo "===== Kafka offsets ====="
  docker exec kafka kafka-run-class kafka.tools.GetOffsetShell \
    --broker-list localhost:9092 \
    --topic cybersecurity-logs \
    --time -1 2>/dev/null || echo "Kafka offset check failed"

  echo
  echo "===== API /threats/top10 ====="
  curl -s --max-time 15 "$API_BASE/threats/top10" -o "$TMP/top10.json"
  python3 - "$TMP/top10.json" <<'PY'
import sys, json
try:
    d = json.load(open(sys.argv[1]))
    for x in d.get("top10", [])[:5]:
        print("{:<18} score={:<4} events={:<4} last_seen={}".format(
            str(x.get("ip")),
            str(x.get("score")),
            str(x.get("total_events")),
            str(x.get("last_seen"))
        ))
except Exception as e:
    print("top10 parse failed:", e)
PY

  echo
  echo "===== API /threats/recent ====="
  curl -s --max-time 15 "$API_BASE/threats/recent" -o "$TMP/recent.json"
  python3 - "$TMP/recent.json" <<'PY'
import sys, json
try:
    d = json.load(open(sys.argv[1]))
    for x in d.get("recent", [])[:5]:
        print("{} | {:<18} | {} | {}".format(
            str(x.get("timestamp")),
            str(x.get("ip")),
            str(x.get("reason")),
            str(x.get("user_agent"))
        ))
except Exception as e:
    print("recent parse failed:", e)
PY

  echo
  echo "===== API /threats/volume-alerts ====="
  curl -s --max-time 15 "$API_BASE/threats/volume-alerts" -o "$TMP/volume.json"
  python3 - "$TMP/volume.json" <<'PY'
import sys, json
try:
    d = json.load(open(sys.argv[1]))
    for x in d.get("volume_alerts", [])[:5]:
        print("{} | {:<18} | bytes={}".format(
            str(x.get("window_end")),
            str(x.get("ip")),
            str(x.get("total_bytes"))
        ))
except Exception as e:
    print("volume parse failed:", e)
PY

  echo
  echo "===== API /threats/threshold ====="
  curl -s --max-time 20 "$API_BASE/threats/threshold" -o "$TMP/threshold.json"
  python3 - "$TMP/threshold.json" <<'PY'
import sys, json
try:
    d = json.load(open(sys.argv[1]))
    print("threshold={} avg_24h={} mode={} samples={}".format(
        d.get("threshold"),
        d.get("avg_score_24h"),
        d.get("mode"),
        d.get("samples_used")
    ))
except Exception as e:
    print("threshold parse failed:", e)
PY

  echo
  echo "============================================================"
  echo "Refreshing every ${INTERVAL}s. Press Ctrl+C to stop."
  sleep "$INTERVAL"
done
