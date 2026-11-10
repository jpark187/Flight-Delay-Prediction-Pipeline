#!/usr/bin/env bash
# faa_poller.sh - save one snapshot of the FAA NAS airport-status feed per minute to GCS.
# Runs as a systemd service on the faa-poller VM (installed by poller_startup.sh).
# Output: gs://BUCKET/stream/faa/dt=YYYY-MM-DD/YYYYMMDDTHHMMSSZ.xml
set -uo pipefail
MD="http://metadata.google.internal/computeMetadata/v1/instance/attributes"
BUCKET=$(curl -fsS -H "Metadata-Flavor: Google" "$MD/bucket")
URL="https://nasstatus.faa.gov/api/airport-status-information"
echo "polling $URL -> $BUCKET/stream/faa/"

while true; do
  ts=$(date -u +%Y%m%dT%H%M%SZ)
  day=$(date -u +%Y-%m-%d)
  if body=$(curl -fsS --max-time 30 "$URL"); then
    if printf '%s' "$body" | gcloud storage cp - "${BUCKET}/stream/faa/dt=${day}/${ts}.xml" --quiet; then
      echo "ok $ts ${#body} bytes"
    else
      echo "upload failed $ts"
    fi
  else
    echo "fetch failed $ts"
  fi
  sleep $(( 60 - 10#$(date +%S) ))        # wake at the top of the next minute
done
