#!/usr/bin/env bash
# run_stage2.sh sample|full - create the cluster, run the three Spark jobs, copy the check
# tables into results/, delete the cluster. Job run times go to results/stage2_timings.csv.
#   KEEP_CLUSTER=1 scripts/run_stage2.sh sample   keeps the cluster for another run
set -euo pipefail
cd "$(dirname "$0")/.."
source env.sh
MODE=${1:-sample}
CLUSTER=${CLUSTER:-flights-spark}
mkdir -p results
[ -f results/stage2_timings.csv ] || echo "utc_end,mode,job,seconds,cluster_shape" > results/stage2_timings.csv
SHAPE=$([ "$MODE" = sample ] && echo "1 x e2-standard-4" || echo "e2-standard-2 + ${WORKERS:-2} x ${WORKER_TYPE:-e2-standard-4}")

scripts/create_cluster.sh "$MODE"

run_job() {
  local job=$1 t0; t0=$(date +%s)
  echo "== $job ($MODE)"
  if ! gcloud dataproc jobs submit pyspark "spark/$job.py" --cluster="$CLUSTER" --region="$REGION" \
        --py-files=spark/common.py -- --base "$BUCKET" --mode "$MODE"; then
    echo "!! $job failed. Fix and rerun; the cluster is kept (auto-deletes after 30 idle min)."
    echo "   Delete it now with: scripts/delete_cluster.sh"
    exit 1
  fi
  echo "$(date -u +%Y-%m-%dT%H:%M:%SZ),$MODE,$job,$(( $(date +%s) - t0 )),$SHAPE" >> results/stage2_timings.csv
}
run_job flights_to_parquet
run_job weather_clean
run_job join_flights_weather

echo "== copying check tables to results/"
rm -rf results/join_coverage results/source_overlap
gcloud storage cp -r "${BUCKET}/curated/checks/*" results/ 2>/dev/null || true
[ "${KEEP_CLUSTER:-0}" = "1" ] || scripts/delete_cluster.sh
echo "== timings"; column -s, -t < results/stage2_timings.csv
