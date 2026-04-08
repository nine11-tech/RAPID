#!/bin/sh
set -e

echo "============================================"
echo "   RAPID - Downloading dataset..."
echo "============================================"

echo ""
echo ">>> [1/2] Installing kaggle..."
pip install --quiet kaggle

echo ""
echo ">>> [2/2] Downloading dataset from Kaggle..."
mkdir -p /root/.kaggle
printf '{"username":"%s","key":"%s"}' "${KAGGLE_USERNAME}" "${KAGGLE_KEY}" > /root/.kaggle/kaggle.json
chmod 600 /root/.kaggle/kaggle.json

mkdir -p /data/raw
if [ ! -f /data/raw/cybersecurity_threat_detection_logs.csv ]; then
  kaggle datasets download -d aryan208/cybersecurity-threat-detection-logs --unzip -p /data/raw/
  echo ">>> Dataset downloaded!"
else
  echo ">>> Dataset already exists, skipping."
fi

echo ""
echo "============================================"
echo "   RAPID - Dataset ready!"
echo "============================================"
