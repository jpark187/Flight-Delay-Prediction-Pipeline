#!/usr/bin/env bash
# delete_loader_vm.sh - delete the loader VM as soon as the full load finishes.
set -euo pipefail
cd "$(dirname "$0")/.."
source env.sh
gcloud compute instances delete flights-loader --zone="$ZONE" --quiet
scripts/log_time.sh DELETE vm/flights-loader
