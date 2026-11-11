#!/usr/bin/env bash
# create_cluster.sh sample|full - Dataproc cluster for the Spark jobs.
#   sample: single node, 4 vCPUs (2024 only)
#   full:   1 master + WORKERS workers (default 2 x e2-standard-4 = 10 vCPUs incl. master);
#           size it to your CPU quota with WORKERS=... WORKER_TYPE=...
# Safety net: the cluster deletes itself after 30 idle minutes (--max-idle).
set -euo pipefail
cd "$(dirname "$0")/.."
source env.sh
MODE=${1:-sample}
CLUSTER=${CLUSTER:-flights-spark}
B=${BUCKET#gs://}
common=(--region="$REGION" --zone="$ZONE" --image-version=2.2-debian12
        --bucket="$B" --temp-bucket="$B" --max-idle=30m
        --master-boot-disk-type=pd-standard --master-boot-disk-size=50GB)
if gcloud dataproc clusters describe "$CLUSTER" --region="$REGION" >/dev/null 2>&1; then
  echo "cluster $CLUSTER already exists"; exit 0
fi
if [ "$MODE" = "sample" ]; then
  gcloud dataproc clusters create "$CLUSTER" "${common[@]}" \
      --single-node --master-machine-type=e2-standard-4
else
  gcloud dataproc clusters create "$CLUSTER" "${common[@]}" \
      --master-machine-type=e2-standard-2 \
      --num-workers="${WORKERS:-2}" --worker-machine-type="${WORKER_TYPE:-e2-standard-4}" \
      --worker-boot-disk-type=pd-balanced --worker-boot-disk-size=100GB
fi
scripts/log_time.sh CREATE "dataproc/$CLUSTER ($MODE)"
