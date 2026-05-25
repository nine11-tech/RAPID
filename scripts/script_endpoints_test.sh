#!/usr/bin/env bash

API_BASE="${API_BASE:-http://100.73.216.115:5000}"
ORIGIN="${ORIGIN:-http://100.97.208.110:3000}"
TEST_IP_HIGH="${TEST_IP_HIGH:-192.168.1.247}"
TEST_IP_PUBLIC="${TEST_IP_PUBLIC:-109.106.120.222}"
TIMEOUT="${TIMEOUT:-60}"

TMP_DIR="/tmp/rapid_endpoint_tests"
mkdir -p "$TMP_DIR"

PASS=0
FAIL=0
WARN=0

pass() { echo "✅ PASS: $1"; PASS=$((PASS+1)); }
fail() { echo "❌ FAIL: $1"; FAIL=$((FAIL+1)); }
warn() { echo "⚠️  WARN: $1"; WARN=$((WARN+1)); }

line() {
  echo "============================================================"
}

need_cmd() {
  command -v "$1" >/dev/null 2>&1 || {
    echo "Missing command: $1"
    exit 1
  }
}

need_cmd curl
need_cmd python3

test_endpoint() {
  local name="$1"
  local path="$2"
  local key="$3"
  local timeout="${4:-$TIMEOUT}"

  local url="${API_BASE}${path}"
  local body="${TMP_DIR}/${name}.json"
  local err="${TMP_DIR}/${name}.err"
  local code

  echo
  echo "### $path"

  code=$(curl -sS \
    --max-time "$timeout" \
    -o "$body" \
    -w "%{http_code}" \
    "$url" 2>"$err")

  if [ "$code" != "200" ]; then
    fail "$path HTTP=$code"
    echo "--- curl error ---"
    cat "$err"
    echo "--- response body ---"
    head -80 "$body"
    return 1
  fi

  python3 - "$body" "$key" <<'PY'
import sys, json

body_path = sys.argv[1]
required_key = sys.argv[2]

try:
    data = json.load(open(body_path, encoding="utf-8"))
except Exception as e:
    print("BAD_JSON", repr(e))
    sys.exit(2)

if isinstance(data, dict) and "error" in data:
    print("API_ERROR:", data.get("error"))
    sys.exit(3)

if required_key != "-" and required_key not in data:
    print("MISSING_KEY:", required_key)
    print("AVAILABLE_KEYS:", list(data.keys()) if isinstance(data, dict) else type(data))
    sys.exit(4)

print("OK_JSON")
PY

  case "$?" in
    0)
      pass "$path HTTP 200 + valid JSON"
      ;;
    2)
      fail "$path returned invalid JSON"
      head -80 "$body"
      ;;
    3)
      fail "$path returned JSON error"
      python3 -m json.tool "$body" 2>/dev/null || cat "$body"
      ;;
    4)
      fail "$path missing required key: $key"
      python3 -m json.tool "$body" 2>/dev/null | head -80 || cat "$body"
      ;;
  esac
}

test_cors() {
  local path="$1"
  local url="${API_BASE}${path}"

  echo
  echo "### CORS $path"

  local cors
  cors=$(curl -sS -i \
    --max-time "$TIMEOUT" \
    -H "Origin: $ORIGIN" \
    "$url" | grep -i "Access-Control-Allow-Origin" | head -1 || true)

  if echo "$cors" | grep -qi "Access-Control-Allow-Origin"; then
    pass "CORS header present: $cors"
  else
    fail "CORS header missing for $path"
  fi

  local options
  options=$(curl -sS -i -X OPTIONS \
    --max-time "$TIMEOUT" \
    -H "Origin: $ORIGIN" \
    -H "Access-Control-Request-Method: GET" \
    "$url" | grep -i "Access-Control-Allow-Methods" | head -1 || true)

  if echo "$options" | grep -qi "Access-Control-Allow-Methods"; then
    pass "CORS preflight OK"
  else
    warn "CORS preflight method header missing"
  fi
}

summary_json() {
  local name="$1"
  local file="${TMP_DIR}/${name}.json"

  [ -s "$file" ] || return

  python3 - "$file" <<'PY'
import sys, json
try:
    d = json.load(open(sys.argv[1], encoding="utf-8"))
except Exception:
    sys.exit(0)

if "status" in d:
    print("status:", d.get("status"))

if "count" in d:
    print("count:", d.get("count"))

if "top10" in d:
    print("top10_count:", len(d.get("top10") or []))
    if d.get("top10"):
        print("top10_first:", d["top10"][0])

if "threshold" in d:
    print("threshold:", d.get("threshold"))
    print("avg_score_24h:", d.get("avg_score_24h"))
    print("window_hours:", d.get("window_hours"))
    print("mode:", d.get("mode"))
    print("samples_used:", d.get("samples_used"))

if "recent" in d:
    print("recent_count:", len(d.get("recent") or []))

if "volume_alerts" in d:
    print("volume_alerts_count:", len(d.get("volume_alerts") or []))

if "by_protocol" in d:
    print("protocols:", list((d.get("by_protocol") or {}).keys()))

if "timeline" in d:
    print("timeline_days:", d.get("days"))
    print("timeline_count:", len(d.get("timeline") or []))

if "attacks" in d:
    print("geo_mode:", d.get("mode"))
    print("geo_count:", len(d.get("attacks") or []))
    print("geo_skipped:", d.get("skipped"))
    if d.get("attacks"):
        a = d["attacks"][0]
        print("first_attack:", {
            "source_ip": a.get("source_ip"),
            "source_country": a.get("source_country"),
            "target_ip": a.get("target_ip"),
            "target_country": a.get("target_country"),
            "attack_type": a.get("attack_type"),
            "score": a.get("score"),
            "severity": a.get("severity"),
        })

if "adaptive_threshold" in d:
    at = d.get("adaptive_threshold") or {}
    print("ip:", d.get("ip"))
    print("final_score:", d.get("final_score"))
    print("threat_level:", d.get("threat_level"))
    print("recommendation:", d.get("recommendation"))
    print("adaptive_threshold:", at.get("threshold"))
    print("avg_score_24h:", at.get("avg_score_24h"))
    print("window_hours:", at.get("window_hours"))
PY
}

line
echo "RAPID API FULL ENDPOINT TEST"
echo "API_BASE     = $API_BASE"
echo "ORIGIN       = $ORIGIN"
echo "TEST_IP_HIGH = $TEST_IP_HIGH"
echo "TEST_IP_PUBLIC = $TEST_IP_PUBLIC"
echo "TIMEOUT      = $TIMEOUT"
date
line

echo
echo "### Basic reachability"
if curl -sS --max-time 10 "$API_BASE/health" >/dev/null; then
  pass "API reachable"
else
  fail "API unreachable: $API_BASE"
  echo "Try: curl -v $API_BASE/health"
  exit 1
fi

test_cors "/health"
test_cors "/threats/top10"

line
echo "CORE API ENDPOINTS"

test_endpoint "health" "/health" "status" 15
summary_json "health"

test_endpoint "top10" "/threats/top10" "top10" 60
summary_json "top10"

test_endpoint "threshold" "/threats/threshold" "threshold" 90
summary_json "threshold"

test_endpoint "recent" "/threats/recent" "recent" 60
summary_json "recent"

test_endpoint "volume_alerts" "/threats/volume-alerts" "volume_alerts" 60
summary_json "volume_alerts"

test_endpoint "by_protocol" "/threats/by-protocol" "by_protocol" 60
summary_json "by_protocol"

test_endpoint "timeline" "/threats/timeline" "timeline" 60
summary_json "timeline"

line
echo "IP DETAIL ENDPOINTS"

test_endpoint "ip_high" "/threats/ip/${TEST_IP_HIGH}" "threat_level" 120
summary_json "ip_high"

test_endpoint "ip_public" "/threats/ip/${TEST_IP_PUBLIC}" "threat_level" 120
summary_json "ip_public"

line
echo "GEO THREAT MAP ENDPOINT"

test_endpoint "geo_attacks" "/threats/geo/attacks" "attacks" 120
summary_json "geo_attacks"

line
echo "BONUS DYNAMIC THRESHOLD VALIDATION"

python3 - <<'PY'
import json, sys, os

threshold_path = "/tmp/rapid_endpoint_tests/threshold.json"
ip_path = "/tmp/rapid_endpoint_tests/ip_high.json"

try:
    t = json.load(open(threshold_path))
    ip = json.load(open(ip_path))
except Exception as e:
    print("❌ FAIL: Could not validate dynamic threshold:", repr(e))
    sys.exit(1)

ok = True

required = ["threshold", "avg_score_24h", "window_hours", "mode", "recalculation"]
for k in required:
    if k not in t:
        print("❌ Missing in /threats/threshold:", k)
        ok = False

if t.get("mode") != "rolling_24h":
    print("❌ mode is not rolling_24h:", t.get("mode"))
    ok = False

if t.get("window_hours") != 24:
    print("❌ window_hours is not 24:", t.get("window_hours"))
    ok = False

at = ip.get("adaptive_threshold") or {}
if "threshold" not in at:
    print("❌ /threats/ip does not include adaptive_threshold.threshold")
    ok = False

score = ip.get("final_score", 0)
thr = at.get("threshold", t.get("threshold", 0))
level = ip.get("threat_level")

print("threshold =", thr)
print("final_score =", score)
print("threat_level =", level)

if score > thr and level != "HIGH":
    print("❌ expected HIGH because final_score > threshold")
    ok = False

if ok:
    print("✅ PASS: Dynamic threshold integrated correctly")
    sys.exit(0)
else:
    sys.exit(1)
PY

if [ "$?" -eq 0 ]; then
  PASS=$((PASS+1))
else
  FAIL=$((FAIL+1))
fi

line
echo "OPTIONAL HDFS API ENDPOINTS"

for path in \
  /hdfs/views \
  /hdfs/views/top10 \
  /hdfs/views/timeline \
  /hdfs/views/top10/files \
  /hdfs/views/timeline/files
do
  name=$(echo "$path" | tr '/' '_' | sed 's/^_//')
  code=$(curl -sS --max-time 20 -o "${TMP_DIR}/${name}.json" -w "%{http_code}" "${API_BASE}${path}" 2>/dev/null || true)

  if [ "$code" = "200" ]; then
    pass "$path exists"
  elif [ "$code" = "404" ]; then
    warn "$path not present in current app.py"
  else
    warn "$path returned HTTP=$code"
  fi
done

line
echo "FINAL SUMMARY"
echo "PASS = $PASS"
echo "WARN = $WARN"
echo "FAIL = $FAIL"
echo "Saved responses: $TMP_DIR"

if [ "$FAIL" -eq 0 ]; then
  echo "🎉 ALL REQUIRED ENDPOINT TESTS PASSED"
  exit 0
else
  echo "🔴 SOME REQUIRED TESTS FAILED"
  exit 1
fi
