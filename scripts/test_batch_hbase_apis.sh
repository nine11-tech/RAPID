#!/usr/bin/env bash

API_BASE="${API_BASE:-http://100.73.216.115:5000}"
TMP="/tmp/rapid_batch_api_tests"
mkdir -p "$TMP"

PASS=0
FAIL=0
WARN=0

pass(){ echo "✅ PASS: $1"; PASS=$((PASS+1)); }
fail(){ echo "❌ FAIL: $1"; FAIL=$((FAIL+1)); }
warn(){ echo "⚠️  WARN: $1"; WARN=$((WARN+1)); }

need_cmd() {
  command -v "$1" >/dev/null 2>&1 || {
    echo "Missing command: $1"
    exit 1
  }
}

need_cmd curl
need_cmd python3

echo "============================================================"
echo "RAPID BATCH HBASE API TESTS ONLY"
echo "API_BASE=$API_BASE"
date
echo "============================================================"

fetch() {
  local name="$1"
  local path="$2"
  local outfile="$TMP/$name.json"
  local code

  code=$(curl -sS --max-time 60 -o "$outfile" -w "%{http_code}" "$API_BASE$path" 2>"$TMP/$name.err" || echo "000")

  if [ "$code" != "200" ]; then
    fail "$path HTTP=$code"
    echo "--- curl err ---"
    cat "$TMP/$name.err" 2>/dev/null || true
    echo "--- body ---"
    head -40 "$outfile" 2>/dev/null || true
    return 1
  fi

  python3 - "$outfile" <<'PY'
import sys, json
try:
    d = json.load(open(sys.argv[1]))
except Exception as e:
    print("BAD_JSON:", repr(e))
    sys.exit(2)

if isinstance(d, dict) and d.get("status") == "error":
    print("API_ERROR:", d.get("error"))
    sys.exit(3)

print("OK_JSON")
PY

  case "$?" in
    0) return 0 ;;
    2) fail "$path invalid JSON"; return 1 ;;
    3) fail "$path returned API error"; python3 -m json.tool "$outfile" 2>/dev/null || cat "$outfile"; return 1 ;;
  esac
}

validate_list_endpoint() {
  local name="$1"
  local path="$2"
  local table="$3"
  local list_key="$4"

  echo
  echo "===== $path ====="

  if ! fetch "$name" "$path"; then
    return
  fi

  python3 - "$TMP/$name.json" "$table" "$list_key" <<'PY'
import sys, json

file_path, expected_table, list_key = sys.argv[1], sys.argv[2], sys.argv[3]
d = json.load(open(file_path))

errors = []

if d.get("status") != "ok":
    errors.append(f"status is {d.get('status')} not ok")

if d.get("layer") != "batch":
    errors.append(f"layer is {d.get('layer')} not batch")

if d.get("source") != "hbase":
    errors.append(f"source is {d.get('source')} not hbase")

if d.get("table") != expected_table:
    errors.append(f"table is {d.get('table')} not {expected_table}")

rows = d.get(list_key)
if not isinstance(rows, list):
    errors.append(f"{list_key} is not a list")
elif len(rows) == 0:
    errors.append(f"{list_key} is empty")
else:
    first = rows[0]
    if "row_key" not in first:
        errors.append("first row missing row_key")

if errors:
    print("VALIDATION_ERRORS:")
    for e in errors:
        print("-", e)
    sys.exit(1)

print("table:", expected_table)
print("rows:", len(rows))
print("first_row_key:", rows[0].get("row_key"))
print("first_keys:", sorted([k for k in rows[0].keys() if k != "columns"])[:20])
PY

  if [ "$?" -eq 0 ]; then
    pass "$path returns batch HBase data for $table"
  else
    fail "$path validation failed"
  fi
}

validate_tables_index() {
  echo
  echo "===== /batch/hbase/tables ====="

  if ! fetch "hbase_tables" "/batch/hbase/tables"; then
    return
  fi

  python3 - "$TMP/hbase_tables.json" <<'PY'
import sys, json

required = {
    "attack_patterns": "/batch/attack-patterns",
    "ip_reputation": "/batch/ip-reputation",
    "multistep_attacks": "/batch/multistep-attacks",
    "port_scans": "/batch/port-scans",
    "threat_timeline": "/batch/threat-timeline",
    "threat_volume": "/batch/threat-volume",
}

d = json.load(open(sys.argv[1]))
tables = d.get("tables", [])

found = {t.get("table"): t for t in tables}
errors = []

for table, endpoint in required.items():
    item = found.get(table)
    if not item:
        errors.append(f"missing table entry {table}")
        continue
    if item.get("endpoint") != endpoint:
        errors.append(f"{table} endpoint is {item.get('endpoint')} not {endpoint}")
    if item.get("exists") is not True:
        errors.append(f"{table} exists is not true")

if errors:
    print("TABLE_INDEX_ERRORS:")
    for e in errors:
        print("-", e)
    sys.exit(1)

print("all required batch HBase tables exist")
for table in required:
    print("-", table, "=>", found[table].get("endpoint"))
PY

  if [ "$?" -eq 0 ]; then
    pass "/batch/hbase/tables lists all batch HBase tables"
  else
    fail "/batch/hbase/tables validation failed"
  fi
}

get_first_row_key() {
  local file="$1"
  local key="$2"
  python3 - "$file" "$key" <<'PY'
import sys, json
d = json.load(open(sys.argv[1]))
rows = d.get(sys.argv[2]) or []
print(rows[0].get("row_key", "") if rows else "")
PY
}

get_first_source_ip() {
  local file="$1"
  local key="$2"
  python3 - "$file" "$key" <<'PY'
import sys, json
d = json.load(open(sys.argv[1]))
rows = d.get(sys.argv[2]) or []
if rows:
    print(rows[0].get("source_ip") or rows[0].get("row_key", "").split("|")[0])
else:
    print("")
PY
}

urlencode() {
  python3 - "$1" <<'PY'
import sys, urllib.parse
print(urllib.parse.quote(sys.argv[1], safe=""))
PY
}

validate_generic_row() {
  local table="$1"
  local row_key="$2"

  if [ -z "$row_key" ]; then
    warn "No sample row key for $table generic row test"
    return
  fi

  local encoded
  encoded=$(urlencode "$row_key")

  echo
  echo "===== /batch/hbase/$table/row?key=$row_key ====="

  if ! fetch "generic_row_$table" "/batch/hbase/$table/row?key=$encoded"; then
    return
  fi

  python3 - "$TMP/generic_row_$table.json" "$table" "$row_key" <<'PY'
import sys, json
d = json.load(open(sys.argv[1]))
table = sys.argv[2]
row_key = sys.argv[3]

if d.get("status") != "ok":
    print("status not ok:", d.get("status"))
    sys.exit(1)
if d.get("table") != table:
    print("wrong table:", d.get("table"))
    sys.exit(1)
row = d.get("row")
if not row or row.get("row_key") != row_key:
    print("wrong/missing row:", row)
    sys.exit(1)

print("row ok:", row.get("row_key"))
PY

  if [ "$?" -eq 0 ]; then
    pass "generic row endpoint works for $table"
  else
    fail "generic row endpoint failed for $table"
  fi
}

validate_direct_ip_endpoint() {
  local name="$1"
  local path="$2"
  local expected_key="$3"

  echo
  echo "===== $path ====="

  if ! fetch "$name" "$path"; then
    return
  fi

  python3 - "$TMP/$name.json" "$expected_key" <<'PY'
import sys, json
d = json.load(open(sys.argv[1]))
key = sys.argv[2]

if d.get("status") not in ("ok", "not_found"):
    print("bad status:", d.get("status"))
    sys.exit(1)

if d.get("status") == "not_found":
    print("not_found is valid only if row does not exist")
    sys.exit(2)

if key not in d:
    print("missing key:", key)
    sys.exit(1)

if not d.get(key):
    print("empty key:", key)
    sys.exit(1)

print("status:", d.get("status"))
print("key:", key)
PY

  rc="$?"
  if [ "$rc" -eq 0 ]; then
    pass "$path direct endpoint works"
  elif [ "$rc" -eq 2 ]; then
    warn "$path returned not_found for sample"
  else
    fail "$path direct endpoint validation failed"
  fi
}

validate_tables_index

echo
echo "============================================================"
echo "LIST ENDPOINTS"
echo "============================================================"

validate_list_endpoint "attack_patterns" "/batch/attack-patterns?limit=5" "attack_patterns" "attack_patterns"
validate_list_endpoint "ip_reputation" "/batch/ip-reputation?limit=5" "ip_reputation" "ip_reputation"
validate_list_endpoint "multistep_attacks" "/batch/multistep-attacks?limit=5" "multistep_attacks" "multistep_attacks"
validate_list_endpoint "port_scans" "/batch/port-scans?limit=5" "port_scans" "port_scans"
validate_list_endpoint "port_scans_top" "/batch/port-scans/top?limit=5" "port_scans" "port_scans"
validate_list_endpoint "threat_timeline" "/batch/threat-timeline?limit=5" "threat_timeline" "threat_timeline"
validate_list_endpoint "threat_volume" "/batch/threat-volume?limit=5" "threat_volume" "threat_volume"

echo
echo "============================================================"
echo "GENERIC /batch/hbase/<table> ENDPOINTS"
echo "============================================================"

validate_list_endpoint "generic_attack_patterns" "/batch/hbase/attack_patterns?limit=5" "attack_patterns" "attack_patterns"
validate_list_endpoint "generic_ip_reputation" "/batch/hbase/ip_reputation?limit=5" "ip_reputation" "ip_reputation"
validate_list_endpoint "generic_multistep_attacks" "/batch/hbase/multistep_attacks?limit=5" "multistep_attacks" "multistep_attacks"
validate_list_endpoint "generic_port_scans" "/batch/hbase/port_scans?limit=5" "port_scans" "port_scans"
validate_list_endpoint "generic_threat_timeline" "/batch/hbase/threat_timeline?limit=5" "threat_timeline" "threat_timeline"
validate_list_endpoint "generic_threat_volume" "/batch/hbase/threat_volume?limit=5" "threat_volume" "threat_volume"

echo
echo "============================================================"
echo "ROW / IP ENDPOINTS"
echo "============================================================"

ATTACK_KEY=$(get_first_row_key "$TMP/attack_patterns.json" "attack_patterns")
IP_REP_KEY=$(get_first_row_key "$TMP/ip_reputation.json" "ip_reputation")
MULTI_IP=$(get_first_row_key "$TMP/multistep_attacks.json" "multistep_attacks")
PORT_IP=$(get_first_source_ip "$TMP/port_scans.json" "port_scans")
TIMELINE_KEY=$(get_first_row_key "$TMP/threat_timeline.json" "threat_timeline")
VOLUME_KEY=$(get_first_row_key "$TMP/threat_volume.json" "threat_volume")

validate_generic_row "attack_patterns" "$ATTACK_KEY"
validate_generic_row "ip_reputation" "$IP_REP_KEY"
validate_generic_row "multistep_attacks" "$MULTI_IP"
validate_generic_row "port_scans" "$(get_first_row_key "$TMP/port_scans.json" "port_scans")"
validate_generic_row "threat_timeline" "$TIMELINE_KEY"
validate_generic_row "threat_volume" "$VOLUME_KEY"

validate_direct_ip_endpoint "direct_ip_reputation" "/batch/ip-reputation/$IP_REP_KEY" "reputation"
validate_direct_ip_endpoint "direct_multistep" "/batch/multistep-attacks/ip/$MULTI_IP" "multistep_attack"
validate_direct_ip_endpoint "direct_port_scans" "/batch/port-scans/ip/$PORT_IP?limit=1000" "port_scans"

echo
echo "============================================================"
echo "FINAL SUMMARY"
echo "PASS=$PASS"
echo "WARN=$WARN"
echo "FAIL=$FAIL"
echo "Responses saved in: $TMP"
echo "============================================================"

if [ "$FAIL" -eq 0 ]; then
  echo "🎉 ALL REQUIRED BATCH HBASE API TESTS PASSED"
  exit 0
else
  echo "🔴 SOME BATCH HBASE API TESTS FAILED"
  exit 1
fi
