#!/usr/bin/env bash
# load_expo.sh - fill the years BTS PREZIP no longer serves, from the ASA Data Expo 2009
# airline dataset on Harvard Dataverse (BTS data, Oct 1987 - Apr 2008, one bz2 CSV per year,
# 29 columns). Spark reads .bz2 directly, so files are stored as-is:
#   gs://BUCKET/raw_expo/YYYY.csv.bz2
# Usage:  START_YEAR=1990 END_YEAR=1997 loaders/load_expo.sh
# Spark (Stage 2) maps the 29 Expo columns to BTS names and uses Expo rows only for months
# that BTS is missing; overlapping months are used to check the two sources agree.
set -euo pipefail
source "$(dirname "$0")/../env.sh"
START_YEAR=${START_YEAR:?set START_YEAR}
END_YEAR=${END_YEAR:?set END_YEAR}
DV="https://dataverse.harvard.edu"
DOI="doi:10.7910/DVN/HG7NV7"

# year -> Dataverse file id, from the dataset's file list
IDS=$(curl -fsSL --retry 5 --retry-delay 10 "$DV/api/datasets/:persistentId/?persistentId=$DOI" | python3 -c '
import json, re, sys
for f in json.load(sys.stdin)["data"]["latestVersion"]["files"]:
    m = re.fullmatch(r"(\d{4})\.csv\.bz2", f["dataFile"]["filename"])
    if m: print(m.group(1), f["dataFile"]["id"])
')
echo "Data Expo years available: $(echo "$IDS" | cut -d" " -f1 | tr "\n" " ")"

WORK=$(mktemp -d); trap 'rm -rf "$WORK"' EXIT
for year in $(seq "$START_YEAR" "$END_YEAR"); do
  dest="${BUCKET}/raw_expo/${year}.csv.bz2"
  if gcloud storage ls "$dest" >/dev/null 2>&1; then echo "skip ${year} (already in GCS)"; continue; fi
  id=$(echo "$IDS" | awk -v y="$year" '$1==y {print $2}')
  [ -n "$id" ] || { echo "  no Data Expo file for ${year}"; continue; }
  echo "fetch ${year} (Dataverse file ${id})"
  curl -fsSL --retry 5 --retry-delay 10 --retry-connrefused -o "$WORK/${year}.csv.bz2" "$DV/api/access/datafile/${id}"
  gcloud storage cp "$WORK/${year}.csv.bz2" "$dest"
  rm -f "$WORK/${year}.csv.bz2"
done
echo "done: $(gcloud storage ls "${BUCKET}/raw_expo/" 2>/dev/null | wc -l) Data Expo year files in ${BUCKET}/raw_expo/"
