#!/usr/bin/env bash
set -euo pipefail

DATA_DIR="data"
ZIP_FILE="${DATA_DIR}/aisdk-2026-04-18.zip"
CSV_FILE="${DATA_DIR}/aisdk-2026-04-18.csv"
DATA_URL="http://aisdata.ais.dk/aisdk-2026-04-18.zip"

mkdir -p "${DATA_DIR}"

if [ -f "${CSV_FILE}" ]; then
  echo "CSV already exists: ${CSV_FILE}"
  exit 0
fi

if [ ! -f "${ZIP_FILE}" ]; then
  echo "Downloading AIS dataset..."
  curl -L -o "${ZIP_FILE}" "${DATA_URL}"
else
  echo "ZIP already exists: ${ZIP_FILE}"
fi

echo "Extracting dataset..."
unzip -o "${ZIP_FILE}" -d "${DATA_DIR}"

echo "Dataset is ready in ${DATA_DIR}"
