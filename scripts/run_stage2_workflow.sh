#!/usr/bin/env bash
# run_stage2_workflow.sh sample|full - run Stage 2 as a Dataproc WORKFLOW, entirely inside
# Google Cloud: Dataproc creates the cluster, runs the jobs in order, and deletes the cluster,
# even if this terminal is closed. Use this for the long full run.
#   flights_to_parquet ─┐
#                       ├─> join_flights_weather
#   weather_clean ──────┘        (the first two are independent, so they run side by side)
# Check progress / collect results later with: scripts/stage2_status.sh
set -euo pipefail
cd "$(dirname "$0")/.."
source env.sh
MODE=${1:-full}
T=stage2-$MODE
B=${BUCKET#gs://}
CODE="${BUCKET}/code/spark"

echo "== uploading Spark code to ${CODE}/"
gcloud storage cp spark/*.py "${CODE}/"

echo "== (re)creating workflow template $T"
gcloud dataproc workflow-templates delete "$T" --region="$REGION" --quiet >/dev/null 2>&1 || true
gcloud dataproc workflow-templates create "$T" --region="$REGION"

if [ "$MODE" = "sample" ]; then
  SHAPE=(--single-node --master-machine-type=e2-standard-4)
else
  SHAPE=(--master-machine-type=e2-standard-2 --num-workers="${WORKERS:-2}"
         --worker-machine-type="${WORKER_TYPE:-e2-standard-4}"
         --worker-boot-disk-type=pd-balanced --worker-boot-disk-size=100GB)
fi
gcloud dataproc workflow-templates set-managed-cluster "$T" --region="$REGION" \
    --cluster-name=flights-wf --zone="$ZONE" --image-version=2.2-debian12 \
    --bucket="$B" --temp-bucket="$B" \
    --master-boot-disk-type=pd-standard --master-boot-disk-size=50GB "${SHAPE[@]}"

add() {   # add STEP_ID JOB [PREREQS]
  local extra=(); [ -n "${3:-}" ] && extra=(--start-after="$3")
  gcloud dataproc workflow-templates add-job pyspark "${CODE}/$2.py" \
      --workflow-template="$T" --region="$REGION" --step-id="$1" \
      --py-files="${CODE}/common.py" "${extra[@]}" -- --base "$BUCKET" --mode "$MODE"
}
add flights flights_to_parquet
add weather weather_clean
add join    join_flights_weather flights,weather

echo "== starting workflow (runs in Google Cloud; safe to close this terminal)"
gcloud dataproc workflow-templates instantiate "$T" --region="$REGION" --async
scripts/log_time.sh CREATE "dataproc/flights-wf ($MODE, workflow; cluster auto-deleted at end)"
cat << MSG

Started. Check on it any time (from Cloud Shell, repo folder):
  scripts/stage2_status.sh
When every job shows DONE, the same command copies timings and check tables into results/.
MSG
