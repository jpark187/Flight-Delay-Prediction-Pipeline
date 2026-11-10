#!/usr/bin/env bash
# cleanup.sh - delete everything this project created.
# Later stages add Cloud Run services and the Atlas cluster here.
set -uo pipefail
cd "$(dirname "$0")/.."
source env.sh

scripts/delete_cluster.sh
for vm in flights-loader faa-poller; do
  if gcloud compute instances describe "$vm" --zone="$ZONE" >/dev/null 2>&1; then
    gcloud compute instances delete "$vm" --zone="$ZONE" --quiet && scripts/log_time.sh DELETE "vm/$vm"
  fi
done

read -r -p "Also delete bucket $BUCKET and ALL data in it? [y/N] " ans
if [ "${ans:-N}" = "y" ]; then
  gcloud storage rm -r "$BUCKET" && scripts/log_time.sh DELETE "bucket/${BUCKET#gs://}"
fi
echo "remaining VMs:"; gcloud compute instances list
