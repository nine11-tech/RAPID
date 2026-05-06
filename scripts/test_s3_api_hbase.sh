#!/usr/bin/env bash

ANASS_IP="${ANASS_IP:-100.73.216.115}"
CHAWI_IP="${CHAWI_IP:-100.97.208.110}"
API="http://${ANASS_IP}:5000"

ISSUES=0
WAITS=0

pass() { echo "✅ PASS: $1"; }
fail() { echo "❌ FAIL: $1"; ISSUES=$((ISSUES+1)); }
waitmsg() { echo "⏳ WAIT: $1"; WAITS=$((WAITS+1)); }
info() { echo "ℹ️  $1"; }

echo "=================================================="
echo "RAPID Sprint 3 API + HBase validation"
date
echo "API       = $API"
echo "CHAWI_IP  = $CHAWI_IP"
echo "=================================================="

echo
echo "### 1) Flask API container"
if docker ps --format '{{.Names}}' | grep -qx "flask-api"; then
  pass "flask-api container is running"
else
  fail "flask-api container is not running. Fix: docker compose --profile anass up -d flask-api"
fi

echo
echo "### 2) API endpoint checks"
for endpoint in \
  /health \
  /threats/top10 \
  /threats/threshold \
  /threats/recent \
  /threats/volume-alerts \
  /threats/by-protocol \
  /threats/timeline \
  /threats/ip/42.119.98.70
do
  code=$(curl -s -o /tmp/rapid_api_test.json -w "%{http_code}" --max-time 10 "$API$endpoint")
  if [ "$code" = "200" ]; then
    pass "API $endpoint returned HTTP 200"
  else
    fail "API $endpoint returned HTTP $code"
    echo "Response:"
    cat /tmp/rapid_api_test.json
    echo
  fi
done

echo
echo "### 3) Cassandra network"
if nc -zv -w 3 "$CHAWI_IP" 9042 >/tmp/cassandra_nc.log 2>&1; then
  pass "Cassandra port 9042 reachable"
else
  fail "Cassandra port 9042 not reachable. Chawi Cassandra/Tailscale/firewall problem."
  cat /tmp/cassandra_nc.log
fi

echo
echo "### 4) Cassandra driver + required tables"
docker exec flask-api python3 - <<PY
from cassandra.cluster import Cluster
import sys

host = "$CHAWI_IP"
required = ["logs", "realtime_threats", "signature_alerts", "threat_scores", "volume_alerts"]

try:
    cluster = Cluster([host])
    session = cluster.connect("cybersecurity")
    tables = sorted([r.name for r in session.execute("DESCRIBE TABLES")])
    print("CASSANDRA_TABLES=" + ",".join(tables))

    missing = [t for t in required if t not in tables]
    if missing:
        print("FAIL_MISSING_TABLES=" + ",".join(missing))
        sys.exit(2)

    for t in required:
        rows = list(session.execute(f"SELECT * FROM {t} LIMIT 1"))
        if not rows:
            print("WAIT_EMPTY_TABLE=" + t)

    cluster.shutdown()
    sys.exit(0)

except Exception as e:
    print("FAIL_CASSANDRA_DRIVER=" + repr(e))
    sys.exit(1)
PY

case $? in
  0) pass "Cassandra driver OK and required tables exist" ;;
  1) fail "flask-api cannot connect to Cassandra driver/keyspace cybersecurity" ;;
  2) fail "Some Cassandra tables are missing" ;;
esac

echo
echo "### 5) HDFS batch output from Hamza"
if docker exec namenode bash -lc "/opt/hadoop/bin/hdfs dfs -test -e /logs/year=2024/month=12/data.csv" >/dev/null 2>&1; then
  pass "HDFS raw logs exist"
else
  fail "HDFS raw logs missing: /logs/year=2024/month=12/data.csv"
fi

echo "Recent /data/cybersecurity files:"
docker exec namenode bash -lc "/opt/hadoop/bin/hdfs dfs -ls -R /data/cybersecurity 2>/dev/null | tail -30" || true

if docker exec namenode bash -lc "/opt/hadoop/bin/hdfs dfs -ls -R /data/cybersecurity 2>/dev/null | grep -Ei 'attack|pattern|reputation|top|api|hbase' >/dev/null"; then
  pass "Some Hamza batch/API/HBase-related output exists in HDFS"
else
  waitmsg "No clear Hamza batch output found yet in /data/cybersecurity. He may still be running attack_pattern_detection.py"
fi

echo
echo "### 6) HBase network ports"
if nc -zv -w 3 "$CHAWI_IP" 16010 >/tmp/hbase_16010.log 2>&1; then
  pass "HBase Master UI port 16010 reachable"
else
  fail "HBase Master UI 16010 not reachable"
  cat /tmp/hbase_16010.log
fi

if nc -zv -w 3 "$CHAWI_IP" 9090 >/tmp/hbase_9090.log 2>&1; then
  pass "HBase Thrift port 9090 reachable"
else
  fail "HBase Thrift 9090 not reachable. API cannot use happybase without this."
  cat /tmp/hbase_9090.log
fi

echo
echo "### 7) HBase driver + ip_reputation table"
HBASE_OUT=$(docker exec flask-api python3 - <<PY
import happybase, sys

host = "$CHAWI_IP"

try:
    conn = happybase.Connection(host, port=9090, timeout=5000)
    conn.open()

    tables = [t.decode() if isinstance(t, bytes) else str(t) for t in conn.tables()]
    print("HBASE_TABLES=" + ",".join(tables))

    if "ip_reputation" not in tables:
        print("WAIT_HBASE_TABLE_MISSING=ip_reputation")
        conn.close()
        sys.exit(10)

    table = conn.table("ip_reputation")
    rows = list(table.scan(limit=1))

    if not rows:
        print("WAIT_HBASE_EMPTY=ip_reputation exists but has 0 rows")
        conn.close()
        sys.exit(11)

    key, data = rows[0]
    sample_ip = key.decode() if isinstance(key, bytes) else str(key)

    print("SAMPLE_IP=" + sample_ip)
    print("HBASE_COLUMNS=" + ",".join([(k.decode() if isinstance(k, bytes) else str(k)) for k in data.keys()]))

    score = data.get(b"cf:reputation_score")
    if score is None:
        print("WAIT_HBASE_SCORE_MISSING=cf:reputation_score column missing")
        conn.close()
        sys.exit(12)

    print("HBASE_REPUTATION_SCORE=" + score.decode())
    conn.close()
    sys.exit(0)

except Exception as e:
    print("FAIL_HBASE_DRIVER=" + repr(e))
    sys.exit(20)
PY
)

HBASE_CODE=$?
echo "$HBASE_OUT"

SAMPLE_IP=$(echo "$HBASE_OUT" | sed -n 's/^SAMPLE_IP=//p' | head -1)

case "$HBASE_CODE" in
  0) pass "HBase ip_reputation exists and contains score data" ;;
  10) waitmsg "HBase table ip_reputation does not exist yet. Hamza/Chawi still need to insert batch reputation data." ;;
  11) waitmsg "HBase ip_reputation table exists but is empty. Hamza is probably still running/writing." ;;
  12) waitmsg "HBase has rows but missing cf:reputation_score. Column name in API or HBase writer must be aligned." ;;
  20) fail "HappyBase driver failed. HBase Thrift issue or protocol/port mismatch." ;;
  *) fail "Unknown HBase test error code $HBASE_CODE" ;;
esac

echo
echo "### 8) API merge test: /threats/ip/<sample from HBase>"
if [ -n "$SAMPLE_IP" ]; then
  curl -s "$API/threats/ip/$SAMPLE_IP" > /tmp/rapid_ip_merge.json

  if python3 - <<'PY'
import json, sys
data = json.load(open("/tmp/rapid_ip_merge.json"))
hs = data.get("historical_score")
if hs is None:
    sys.exit(1)
print(hs)
PY
  then
    pass "API returns historical_score from HBase for IP=$SAMPLE_IP"
  else
    waitmsg "API reached /threats/ip/$SAMPLE_IP but historical_score is still null. Check HBase column cf:reputation_score or app.py mapping."
    echo "API response:"
    cat /tmp/rapid_ip_merge.json
    echo
  fi
else
  waitmsg "Skipping API HBase merge test because no HBase sample IP exists yet."
fi

echo
echo "=================================================="
echo "SUMMARY"
echo "FAIL issues : $ISSUES"
echo "WAIT states : $WAITS"

if [ "$ISSUES" -eq 0 ] && [ "$WAITS" -eq 0 ]; then
  echo "🎉 ALL GOOD: Sprint 3 API + HBase integration is complete."
  exit 0
elif [ "$ISSUES" -eq 0 ]; then
  echo "🟡 INFRA OK, but waiting for Hamza/HBase data to finish."
  exit 10
else
  echo "🔴 FIX FAIL ITEMS FIRST."
  exit 1
fi
