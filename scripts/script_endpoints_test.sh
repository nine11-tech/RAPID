#!/usr/bin/env bash

API_BASE="${API_BASE:-http://100.73.216.115:5000}"
ORIGIN="${ORIGIN:-http://100.97.208.110:3000}"
TIMEOUT="${TIMEOUT:-30}"

PASS=0
FAIL=0
WARN=0

TMP_DIR="/tmp/rapid_api_tests"
mkdir -p "$TMP_DIR"

green() { echo -e "✅ PASS: $1"; PASS=$((PASS+1)); }
red()   { echo -e "❌ FAIL: $1"; FAIL=$((FAIL+1)); }
yellow(){ echo -e "⚠️  WARN: $1"; WARN=$((WARN+1)); }
info()  { echo -e "ℹ️  $1"; }

line() {
  echo "============================================================"
}

need_cmd() {
  command -v "$1" >/dev/null 2>&1 || {
    red "Missing command: $1"
    echo "Install it then rerun."
    exit 1
  }
}

need_cmd curl
need_cmd python3

test_json_endpoint() {
  local name="$1"
  local path="$2"
  local required_key="$3"
  local max_time="${4:-$TIMEOUT}"

  local url="${API_BASE}${path}"
  local body="${TMP_DIR}/${name}.json"
  local headers="${TMP_DIR}/${name}.headers"
  local code

  echo
  echo "### Testing $path"

  code=$(curl -sS \
    --max-time "$max_time" \
    -D "$headers" \
    -o "$body" \
    -w "%{http_code}" \
    "$url" 2>"${TMP_DIR}/${name}.curlerr")

  if [ "$code" != "200" ]; then
    red "$path returned HTTP $code"
    echo "--- curl error ---"
    cat "${TMP_DIR}/${name}.curlerr"
    echo "--- response body ---"
    cat "$body"
    echo
    return
  fi

  python3 - "$body" "$required_key" <<'PY'
import sys, json
path = sys.argv[1]
required = sys.argv[2]

try:
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
except Exception as e:
    print("BAD_JSON:", repr(e))
    sys.exit(2)

if isinstance(data, dict) and "error" in data:
    print("API_ERROR:", data.get("error"))
    sys.exit(3)

if required != "-" and isinstance(data, dict) and required not in data:
    print("MISSING_KEY:", required)
    print("AVAILABLE_KEYS:", list(data.keys()))
    sys.exit(4)

if required != "-" and isinstance(data, dict):
    val = data.get(required)
    if isinstance(val, list):
        print(f"OK_JSON key={required} list_count={len(val)}")
    elif isinstance(val, dict):
        print(f"OK_JSON key={required} dict_keys={list(val.keys())[:10]}")
    else:
        print(f"OK_JSON key={required} value={val}")
else:
    print("OK_JSON")
PY

  case $? in
    0)
      green "$path returned HTTP 200 and valid JSON"
      ;;
    2)
      red "$path returned non-JSON"
      echo "--- body preview ---"
      head -40 "$body"
      ;;
    3)
      red "$path returned JSON error"
      cat "$body" | python3 -m json.tool 2>/dev/null || cat "$body"
      ;;
    4)
      red "$path JSON missing required key: $required_key"
      cat "$body" | python3 -m json.tool 2>/dev/null | head -80 || cat "$body"
      ;;
  esac
}

test_cors() {
  local path="$1"
  local url="${API_BASE}${path}"

  echo
  echo "### Testing CORS for $path"

  local cors
  cors=$(curl -sS -i \
    --max-time "$TIMEOUT" \
    -H "Origin: $ORIGIN" \
    "$url" | grep -i "Access-Control-Allow-Origin" | head -1 || true)

  if echo "$cors" | grep -qi "Access-Control-Allow-Origin"; then
    green "CORS header present for $path: $cors"
  else
    red "No CORS header for $path. Browser dashboard fetch may fail."
  fi

  local preflight
  preflight=$(curl -sS -i -X OPTIONS \
    --max-time "$TIMEOUT" \
    -H "Origin: $ORIGIN" \
    -H "Access-Control-Request-Method: GET" \
    "$url" | grep -i "Access-Control-Allow-Methods" | head -1 || true)

  if echo "$preflight" | grep -qi "Access-Control-Allow-Methods"; then
    green "CORS preflight OK for $path"
  else
    yellow "CORS preflight header missing for $path"
  fi
}

extract_summary() {
  local name="$1"
  local body="${TMP_DIR}/${name}.json"

  if [ ! -s "$body" ]; then
    return
  fi

  python3 - "$body" <<'PY'
import sys, json
path=sys.argv[1]
try:
    data=json.load(open(path))
except Exception:
    sys.exit(0)

if "count" in data:
    print("count:", data.get("count"))
if "days" in data:
    print("days:", data.get("days"))
if "threshold" in data:
    print("threshold:", data.get("threshold"), "avg:", data.get("avg_score"), "total_ips:", data.get("total_ips"))
if "top10" in data:
    print("top10_first:", data["top10"][0] if data["top10"] else None)
if "attacks" in data:
    print("geo_count:", data.get("count"), "mode:", data.get("mode"))
    print("geo_skipped:", data.get("skipped"))
    print("first_attack:", data["attacks"][0] if data.get("attacks") else None)
if "historical_score" in data or "threat_level" in data:
    print("ip:", data.get("ip"))
    print("historical_score:", data.get("historical_score"))
    print("final_score:", data.get("final_score"))
    print("threat_level:", data.get("threat_level"))
    print("recommendation:", data.get("recommendation"))
PY
}

line
echo "RAPID API endpoint validation"
echo "API_BASE = $API_BASE"
echo "ORIGIN   = $ORIGIN"
echo "TIMEOUT  = $TIMEOUT seconds"
date
line

echo
echo "### Basic reachability"
if curl -sS --max-time 10 "$API_BASE/health" >/dev/null; then
  green "API reachable: $API_BASE"
else
  red "API unreachable: $API_BASE"
  echo "Try:"
  echo "  curl -v $API_BASE/health"
  exit 1
fi

test_cors "/health"
test_cors "/threats/top10"

line
echo "Core dashboard endpoints"

test_json_endpoint "health" "/health" "status" 10
test_json_endpoint "top10" "/threats/top10" "top10" 30
extract_summary "top10"

test_json_endpoint "threshold" "/threats/threshold" "threshold" 30
extract_summary "threshold"

test_json_endpoint "recent" "/threats/recent" "recent" 30
test_json_endpoint "volume_alerts" "/threats/volume-alerts" "volume_alerts" 30
test_json_endpoint "by_protocol" "/threats/by-protocol" "by_protocol" 30
test_json_endpoint "timeline" "/threats/timeline" "timeline" 30
extract_summary "timeline"

line
echo "IP detail endpoint with Cassandra + HBase enrichment"

test_json_endpoint "ip_detail" "/threats/ip/109.106.120.222" "threat_level" 40
extract_summary "ip_detail"

line
echo "HDFS archive endpoints"

test_json_endpoint "hdfs_views" "/hdfs/views" "views" 20
test_json_endpoint "hdfs_top10" "/hdfs/views/top10" "hdfs_uri" 20
test_json_endpoint "hdfs_timeline" "/hdfs/views/timeline" "hdfs_uri" 20
test_json_endpoint "hdfs_top10_files" "/hdfs/views/top10/files" "parquet_files" 20
test_json_endpoint "hdfs_timeline_files" "/hdfs/views/timeline/files" "parquet_files" 20

line
echo "Threat map / Geo endpoint"

test_json_endpoint "geo_attacks" "/threats/geo/attacks" "attacks" 60
extract_summary "geo_attacks"

line
echo "Final summary"
echo "PASS = $PASS"
echo "WARN = $WARN"
echo "FAIL = $FAIL"

if [ "$FAIL" -eq 0 ]; then
  echo "🎉 ALL REQUIRED API TESTS PASSED"
  exit 0
else
  echo "🔴 SOME API TESTS FAILED"
  echo "Saved responses in: $TMP_DIR"
  exit 1
fi
