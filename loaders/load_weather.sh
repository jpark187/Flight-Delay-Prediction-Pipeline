#!/usr/bin/env bash
# load_weather.sh - download NOAA GHCNh hourly weather (station-year Parquet) for every airport in
# reference/airport_station_map.csv and upload to GCS.
#   weather/raw/YYYY/GHCNh_<station>_YYYY.parquet   the observations
#   weather/manifest/YYYY.csv                       iata,year,ghcn_id actually used that year
# For each airport and year, candidate stations are tried nearest-airport-station first; the
# first one with data that year wins. Stations open, close and get renumbered over 38 years.
# Usage:   ./load_weather.sh                                              (1987-2025, all airports)
#          START_YEAR=2024 END_YEAR=2024 AIRPORTS=ORD,ATL,SFO ./load_weather.sh   (sample)
# Resumable: years finished by a full run are skipped (marker file _COMPLETE).
set -euo pipefail

source "$(dirname "$0")/../env.sh"           # BUCKET=gs://...
START_YEAR=${START_YEAR:-1987}
END_YEAR=${END_YEAR:-2025}
AIRPORTS=${AIRPORTS:-}                        # optional comma-separated IATA filter
PARALLEL=${PARALLEL:-8}

# NOAA Open Data mirror on AWS (no credentials, fast), with NCEI itself as fallback.
export S3_BASE="https://noaa-ghcnh-pds.s3.amazonaws.com/hourly/access/by-year"
export NCEI_BASE="https://www.ncei.noaa.gov/oa/global-historical-climatology-network/hourly/access/by-year"

MAP="$(dirname "$0")/../reference/airport_station_map.csv"
[ -f "$MAP" ] || { echo "run loaders/build_station_map.py first"; exit 1; }

# One line per selected airport: "IATA cand1;cand2;..." (airports with no candidates skipped).
TARGETS=$(python3 - "$MAP" "$AIRPORTS" << 'PY'
import csv, sys
want = {a.strip().upper() for a in sys.argv[2].split(",") if a.strip()}
for r in csv.DictReader(open(sys.argv[1])):
    if r.get("candidates") and (not want or r["iata"] in want):
        print(r["iata"], r["candidates"])
PY
)
echo "$(echo "$TARGETS" | grep -c .) airports selected"

fetch_airport() {   # $1=IATA $2=candidates ; uses YEAR, OUTDIR
  local iata=$1 cands=$2 s f tmp
  for s in ${cands//;/ }; do
    f="GHCNh_${s}_${YEAR}.parquet"
    if [ -s "$OUTDIR/$f" ]; then echo "$iata,$YEAR,$s" > "$OUTDIR/m/$iata"; return; fi
    tmp="$OUTDIR/.tmp.$iata.$f"
    if curl -fsSL --retry 3 --retry-delay 5 -o "$tmp" "$S3_BASE/$YEAR/parquet/$f" 2>/dev/null \
       || curl -fsSL --retry 3 --retry-delay 5 -o "$tmp" "$NCEI_BASE/$YEAR/parquet/$f" 2>/dev/null; then
      mv "$tmp" "$OUTDIR/$f"
      echo "$iata,$YEAR,$s" > "$OUTDIR/m/$iata"
      return
    fi
    rm -f "$tmp"                               # no file for this station-year: try the next one
  done
  echo "$iata,$YEAR," > "$OUTDIR/m/$iata"      # no candidate had data this year
}
export -f fetch_airport

WORK=$(mktemp -d); trap 'rm -rf "$WORK"' EXIT
for year in $(seq "$START_YEAR" "$END_YEAR"); do
  dest="${BUCKET}/weather/raw/${year}/"
  if [ -z "$AIRPORTS" ] && gcloud storage ls "${dest}_COMPLETE" >/dev/null 2>&1; then
    echo "skip ${year} (full year already in GCS)"; continue
  fi
  export YEAR=$year OUTDIR="$WORK/$year"
  mkdir -p "$OUTDIR/m"
  echo "$TARGETS" | grep . | xargs -P "$PARALLEL" -L 1 bash -c 'fetch_airport "$1" "$2"' _

  manifest="$WORK/manifest_${year}.csv"
  { echo "iata,year,ghcn_id"; cat "$OUTDIR"/m/* | sort; } > "$manifest"
  n_files=$(ls "$OUTDIR"/*.parquet 2>/dev/null | wc -l)
  n_ap=$(tail -n +2 "$manifest" | grep -vc ',$' || true)
  n_all=$(tail -n +2 "$manifest" | wc -l)
  echo "${year}: ${n_files} station files, ${n_ap}/${n_all} airports with weather"
  missing=$(tail -n +2 "$manifest" | { grep ',$' || true; } | cut -d, -f1 | tr '\n' ' ')
  [ -n "$missing" ] && echo "  no weather: ${missing}"

  if [ "$n_files" -gt 0 ]; then
    gcloud storage cp "$OUTDIR"/*.parquet "$dest"
  fi
  if [ -z "$AIRPORTS" ]; then                  # full run: authoritative manifest + done marker
    gcloud storage cp "$manifest" "${BUCKET}/weather/manifest/${year}.csv"
    echo "$n_files station files" | gcloud storage cp - "${dest}_COMPLETE"
  else                                         # sample run: separate manifest, never overwrites full
    gcloud storage cp "$manifest" "${BUCKET}/weather/manifest_sample/${year}.csv"
  fi
  rm -rf "$OUTDIR" "$manifest"
done
