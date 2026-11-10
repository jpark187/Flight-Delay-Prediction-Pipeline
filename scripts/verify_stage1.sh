#!/usr/bin/env bash
# verify_stage1.sh - report what is in the bucket after loading.
set -uo pipefail
cd "$(dirname "$0")/.."
source env.sh
echo "== flights: monthly CSV files"
gcloud storage ls "${BUCKET}/raw/**.csv" 2>/dev/null | wc -l
echo "== flights: years present"
gcloud storage ls "${BUCKET}/raw/" 2>/dev/null | sed -E 's#.*/raw/([0-9]{4})/$#\1#' | tr '\n' ' '; echo
echo "== weather: station-year files"
gcloud storage ls "${BUCKET}/weather/raw/**.parquet" 2>/dev/null | wc -l
echo "== airport map: matched airports"
gcloud storage cat "${BUCKET}/reference/airport_station_map.csv" 2>/dev/null \
  | awk -F, 'NR>1 && $3!="" {m++} NR>1 {t++} END {print m"/"t}'
echo "== FAA snapshots (if poller running)"
gcloud storage ls "${BUCKET}/stream/faa/**.xml" 2>/dev/null | wc -l
echo "== storage used (bytes) by top-level folder"
gcloud storage du -s "${BUCKET}/raw" "${BUCKET}/weather" "${BUCKET}/stream" 2>/dev/null
