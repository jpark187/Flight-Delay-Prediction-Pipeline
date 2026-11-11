#!/usr/bin/env bash
# stage2_status.sh [OPERATION_ID] - progress of the Stage 2 workflow started by
# run_stage2_workflow.sh (operation id saved in results/.last_workflow_op). When it has finished,
# records job run times in results/stage2_timings.csv, copies the check tables into results/
# and logs the cluster deletion in timestamps.txt (once).
set -uo pipefail
cd "$(dirname "$0")/.."
source env.sh
OP=${1:-$(cat results/.last_workflow_op 2>/dev/null)}
[ -n "$OP" ] || { echo "usage: $0 OPERATION_ID (from the 'Instantiating ... operation [...]' line)"; exit 1; }
SHAPE="e2-standard-2 + ${WORKERS:-2} x ${WORKER_TYPE:-e2-standard-4} (workflow)"

gcloud dataproc operations describe "$OP" --region="$REGION" --format=json > /tmp/wf_op.json || exit 1
python3 - << 'PY' > /tmp/wf_jobs.txt
import json
op = json.load(open("/tmp/wf_op.json"))
m = op.get("metadata", {})
print("STATE", m.get("state", "?"), "DONE" if op.get("done") else "NOTDONE",
      "ERROR" if op.get("error") else "OK", m.get("clusterName", "-"))
if op.get("error"):
    print("ERRMSG", op["error"].get("message", "")[:400].replace("\n", " "))
for n in m.get("graph", {}).get("nodes", []):
    print("NODE", n["stepId"], n.get("state", "?"), n.get("jobId", "-"))
PY
read -r _ WSTATE WDONE WERR WCLUSTER < <(grep ^STATE /tmp/wf_jobs.txt)
echo "== workflow $OP: $WSTATE (cluster ${WCLUSTER})"
grep ^ERRMSG /tmp/wf_jobs.txt | cut -d" " -f2-

: > /tmp/wf_rows.csv
while read -r _ step state job; do
  secs="-"; end="-"
  if [ "$job" != "-" ]; then
    gcloud dataproc jobs describe "$job" --region="$REGION" --format=json > /tmp/wf_job.json 2>/dev/null
    read -r end secs < <(python3 - << 'PY'
import json, datetime as dt
j = json.load(open("/tmp/wf_job.json"))
t = lambda s: dt.datetime.fromisoformat(s.replace("Z", "+00:00"))
start = next((h["stateStartTime"] for h in j.get("statusHistory", []) if h["state"] == "RUNNING"), None)
st = j["status"]
end = st["stateStartTime"] if st["state"] in ("DONE", "ERROR", "CANCELLED") else None
print(end or "-", int((t(end) - t(start)).total_seconds()) if start and end else "-")
PY
)
  fi
  printf "  %-8s %-10s %-24s seconds %s\n" "$step" "$state" "$job" "$secs"
  echo "$end,$step,$secs" >> /tmp/wf_rows.csv
done < <(grep ^NODE /tmp/wf_jobs.txt)

if [ "$WDONE" != "DONE" ]; then echo "(still running: check again later)"; exit 0; fi
if [ "$WERR" = "ERROR" ] || grep -q "^NODE .* FAILED" /tmp/wf_jobs.txt; then
  echo "!! workflow failed; see the error above. Dataproc has deleted the cluster."; exit 1
fi

if ! grep -q "workflow $OP" timestamps.txt 2>/dev/null; then
  echo "== finished: recording timings and copying check tables"
  [ -f results/stage2_timings.csv ] || echo "utc_end,mode,job,seconds,cluster_shape" > results/stage2_timings.csv
  declare -A NAME=([flights]=flights_to_parquet [weather]=weather_clean [join]=join_flights_weather [eda]=eda_aggregates)
  while IFS=, read -r end step secs; do
    echo "$end,full,${NAME[$step]:-$step},$secs,$SHAPE" >> results/stage2_timings.csv
  done < /tmp/wf_rows.csv
  rm -rf results/join_coverage results/source_overlap
  gcloud storage cp -r "${BUCKET}/curated/checks/*" results/
  scripts/log_time.sh DELETE "dataproc/${WCLUSTER} (deleted by Dataproc at end of workflow $OP)"
fi
echo "== timings"; column -s, -t < results/stage2_timings.csv
echo "== coverage by year"; column -s, -t < results/join_coverage/*.csv | head -45
