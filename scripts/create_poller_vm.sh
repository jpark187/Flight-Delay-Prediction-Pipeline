#!/usr/bin/env bash
# create_poller_vm.sh - free-tier e2-micro that polls the FAA feed every minute until the demo.
# Free tier needs: e2-micro, us-west1/us-central1/us-east1, standard disk <= 30 GB.
# The external IPv4 address is billed (~$3.65/month); the VM needs it to reach the FAA feed.
set -euo pipefail
cd "$(dirname "$0")/.."
source env.sh
case "$REGION" in us-west1|us-central1|us-east1) ;; *)
  echo "REGION must be us-west1, us-central1 or us-east1 for the free e2-micro"; exit 1 ;; esac
gcloud compute instances create faa-poller \
    --zone="$ZONE" --machine-type=e2-micro \
    --image-family=debian-12 --image-project=debian-cloud \
    --boot-disk-size=10GB --boot-disk-type=pd-standard \
    --scopes=storage-rw \
    --metadata=bucket="$BUCKET" \
    --metadata-from-file=startup-script=stream/poller_startup.sh,poller-script=stream/faa_poller.sh
scripts/log_time.sh CREATE vm/faa-poller
echo "Poller starting. In ~3 minutes check it with: scripts/check_poller.sh"
