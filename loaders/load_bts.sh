#!/usr/bin/env bash
# load_bts.sh - download BTS Reporting Carrier On-Time Performance (Oct 1987 - Dec 2025) into GCS.
# Usage:   ./load_bts.sh                                 (full range)
#          START_YEAR=2024 END_YEAR=2024 ./load_bts.sh   (one-year sample for testing / graders)
# Resumable: months already in GCS are skipped, so just rerun if it stops.
set -euo pipefail

source "$(dirname "$0")/../env.sh"        # must define BUCKET, e.g. BUCKET=gs://my-flights-bucket
START_YEAR=${START_YEAR:-1987}
END_YEAR=${END_YEAR:-2025}
FIRST_YEAR=1987; FIRST_MONTH=10          # BTS on-time data begins October 1987

BTS_BASE="https://transtats.bts.gov/PREZIP"
MIRROR_BASE="https://github.com/dosedaf/bts-ontime-mirror/releases/download"   # fallback only

WORK=$(mktemp -d)
trap 'rm -rf "$WORK"' EXIT
MISSING="$WORK/missing_months.txt"; : > "$MISSING"

for year in $(seq "$START_YEAR" "$END_YEAR"); do
  for month in $(seq 1 12); do
    if [ "$year" -eq "$FIRST_YEAR" ] && [ "$month" -lt "$FIRST_MONTH" ]; then
      continue                           # no data published before Oct 1987
    fi
    mm=$(printf '%02d' "$month")
    dest="${BUCKET}/raw/${year}/${mm}/"

    if gcloud storage ls "${dest}*.csv" >/dev/null 2>&1; then
      echo "skip ${year}-${mm} (already in GCS)"
      continue
    fi

    zip="$WORK/${year}_${mm}.zip"
    bts_url="${BTS_BASE}/On_Time_Reporting_Carrier_On_Time_Performance_1987_present_${year}_${month}.zip"
    mirror_url="${MIRROR_BASE}/${year}_${mm}/${year}_${mm}.zip"

    echo "fetch ${year}-${mm}"
    if ! curl -fsSL --retry 5 --retry-delay 10 --retry-connrefused -o "$zip" "$bts_url"; then
      echo "  BTS download failed, trying mirror"
      if ! curl -fsSL --retry 5 --retry-delay 10 --retry-connrefused -o "$zip" "$mirror_url"; then
        echo "  MISSING ${year}-${mm}: not available from BTS or the mirror; continuing"
        echo "${year}-${mm}" >> "$MISSING"
        rm -f "$zip"
        continue
      fi
    fi

    mkdir -p "$WORK/csv"
    unzip -q -o "$zip" '*.csv' -d "$WORK/csv"
    gcloud storage cp "$WORK/csv/"*.csv "$dest"
    rm -rf "$zip" "$WORK/csv"
    sleep 1                              # be polite to the BTS server
  done
done

echo "done: $(gcloud storage ls "${BUCKET}/raw/**.csv" | wc -l) monthly files in ${BUCKET}/raw/"
if [ -s "$MISSING" ]; then
  echo "months not available this run ($(wc -l < "$MISSING")): $(tr '\n' ' ' < "$MISSING")"
  gcloud storage cp "$MISSING" "${BUCKET}/raw/_missing_months_$(date -u +%Y%m%dT%H%M%SZ).txt"
fi
