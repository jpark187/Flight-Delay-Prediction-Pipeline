#!/usr/bin/env bash
# delete_cluster.sh - delete the Dataproc cluster (run_stage2.sh does this automatically).
set -uo pipefail
cd "$(dirname "$0")/.."
source env.sh
CLUSTER=${CLUSTER:-flights-spark}
if gcloud dataproc clusters describe "$CLUSTER" --region="$REGION" >/dev/null 2>&1; then
  gcloud dataproc clusters delete "$CLUSTER" --region="$REGION" --quiet \
    && scripts/log_time.sh DELETE "dataproc/$CLUSTER"
else
  echo "no cluster $CLUSTER (it may have auto-deleted after 30 idle minutes)"
fi
