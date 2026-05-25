#!/usr/bin/env bash

PORT="${PORT:-8080}"
HOST="${HOST:-0.0.0.0}"
API_BASE="${API_BASE:-http://100.73.216.115:5000}"

echo "============================================================"
echo "RAPID LOCAL DASHBOARD TEST SERVER"
echo "Serving current folder on: http://localhost:${PORT}"
echo "API target: $API_BASE"
echo "============================================================"

echo
echo "Checking required files..."
for f in index.html dashboard.html batch.html main.js batch.js style.css batch.css; do
  if [ -f "$f" ]; then
    echo "✅ $f"
  else
    echo "⚠️  missing: $f"
  fi
done

echo
echo "Checking API..."
curl -s --max-time 10 "$API_BASE/health" | python3 -m json.tool >/dev/null \
  && echo "✅ API reachable" \
  || echo "❌ API unreachable: $API_BASE"

echo
echo "Open these in browser:"
echo "  Batch dashboard:     http://localhost:${PORT}/batch.html"
echo "  Streaming dashboard: http://localhost:${PORT}/dashboard.html"
echo
echo "Press Ctrl+C to stop."
echo "============================================================"

python3 -m http.server "$PORT" --bind "$HOST"
