#!/usr/bin/env bash
# delete_poller_vm.sh - stop collecting the FAA feed (after the demo recording).
set -euo pipefail
cd "$(dirname "$0")/.."
source env.sh
gcloud compute instances delete faa-poller --zone="$ZONE" --quiet
scripts/log_time.sh DELETE vm/faa-poller
