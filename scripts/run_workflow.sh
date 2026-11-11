#!/usr/bin/env bash
# run_workflow.sh STEP SPARK_SCRIPT [MODE] - run one Spark job as a Dataproc workflow: Dataproc
# creates the cluster, runs the job and deletes the cluster, with no open terminal needed.
#   WORKERS=3 scripts/run_workflow.sh features build_features      (Stage 4 feature engineering)
# Check on it with:  WORKERS=3 scripts/stage2_status.sh
set -euo pipefail
cd "$(dirname "$0")/.."
source env.sh
STEP=${1:?step id, e.g. features}; JOB=${2:?spark script name without .py}; MODE=${3:-full}
T="wf-$STEP"
B=${BUCKET#gs://}
CODE="${BUCKET}/code/spark"
[ -f "spark/$JOB.py" ] || { echo "no spark/$JOB.py"; exit 1; }
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
gcloud dataproc workflow-templates add-job pyspark "${CODE}/$JOB.py" \
    --workflow-template="$T" --region="$REGION" --step-id="$STEP" \
    --py-files="${CODE}/common.py" -- --base "$BUCKET" --mode "$MODE" >/dev/null
out=$(gcloud dataproc workflow-templates instantiate "$T" --region="$REGION" --async 2>&1); echo "$out"
mkdir -p results
echo "$out" | grep -o 'operations/[a-z0-9-]*' | head -1 | cut -d/ -f2 > results/.last_workflow_op
scripts/log_time.sh CREATE "dataproc/flights-wf ($T workflow; cluster auto-deleted at end)"
echo "Started $T. Safe to close this terminal. Check with: WORKERS=${WORKERS:-3} scripts/stage2_status.sh"
