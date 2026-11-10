#!/usr/bin/env bash
# check_poller.sh - is the FAA poller writing snapshots?
set -uo pipefail
cd "$(dirname "$0")/.."
source env.sh
day=$(date -u +%Y-%m-%d)
n=$(gcloud storage ls "${BUCKET}/stream/faa/dt=${day}/" 2>/dev/null | wc -l)
echo "snapshots today (UTC ${day}): ${n}"
latest=$(gcloud storage ls "${BUCKET}/stream/faa/dt=${day}/" 2>/dev/null | tail -1)
if [ -n "$latest" ]; then
  echo "latest: $latest"; gcloud storage cat "$latest" | head -c 600; echo
else
  echo "none yet. Poller log:"
  gcloud compute ssh faa-poller --zone="$ZONE" --command="sudo journalctl -u faa-poller -n 20 --no-pager"
fi
