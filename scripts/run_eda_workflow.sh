#!/usr/bin/env bash
# run_eda_workflow.sh - full-history EDA aggregates as a Dataproc workflow (no open terminal
# needed; Dataproc creates and deletes the cluster). Then, when stage2_status.sh shows DONE:
#     python3 eda/eda_full.py        (charts + summary from the small aggregate tables)
set -euo pipefail
cd "$(dirname "$0")/.."
source env.sh
T=eda-full
B=${BUCKET#gs://}
CODE="${BUCKET}/code/spark"
gcloud storage cp spark/*.py "${CODE}/"
gcloud dataproc workflow-templates delete "$T" --region="$REGION" --quiet >/dev/null 2>&1 || true
gcloud dataproc workflow-templates create "$T" --region="$REGION" >/dev/null
gcloud dataproc workflow-templates set-managed-cluster "$T" --region="$REGION" \
    --cluster-name=flights-wf --zone="$ZONE" --image-version=2.2-debian12 \
    --bucket="$B" --temp-bucket="$B" \
    --master-boot-disk-type=pd-standard --master-boot-disk-size=50GB \
    --master-machine-type=e2-standard-2 --num-workers="${WORKERS:-3}" \
    --worker-machine-type="${WORKER_TYPE:-e2-standard-4}" \
    --worker-boot-disk-type=pd-balanced --worker-boot-disk-size=100GB >/dev/null
gcloud dataproc workflow-templates add-job pyspark "${CODE}/eda_aggregates.py" \
    --workflow-template="$T" --region="$REGION" --step-id=eda \
    --py-files="${CODE}/common.py" -- --base "$BUCKET" --mode full >/dev/null
out=$(gcloud dataproc workflow-templates instantiate "$T" --region="$REGION" --async 2>&1); echo "$out"
mkdir -p results
echo "$out" | grep -o 'operations/[a-z0-9-]*' | head -1 | cut -d/ -f2 > results/.last_workflow_op
scripts/log_time.sh CREATE "dataproc/flights-wf (eda-full workflow; cluster auto-deleted at end)"
echo "Started. Check with: WORKERS=${WORKERS:-3} scripts/stage2_status.sh"
