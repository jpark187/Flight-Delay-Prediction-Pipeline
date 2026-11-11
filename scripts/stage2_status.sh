#!/usr/bin/env bash
# stage2_status.sh - progress of the Stage 2 workflow; when finished, record job timings in
# results/stage2_timings.csv and copy the check tables into results/.
set -uo pipefail
cd "$(dirname "$0")/.."
source env.sh

echo "== cluster"
gcloud dataproc clusters list --region="$REGION" --filter="clusterName ~ ^flights-wf" \
    --format="table(clusterName,status.state,config.workerConfig.numInstances)" 2>/dev/null \
  | grep . || echo "(none running: not started yet, or finished and deleted)"

echo "== jobs (most recent workflow)"
gcloud dataproc jobs list --region="$REGION" --filter="placement.clusterName ~ ^flights-wf" \
    --format=json --limit=20 > /tmp/wf_jobs.json 2>/dev/null || echo "[]" > /tmp/wf_jobs.json
python3 - << 'PY'
import json, datetime as dt
jobs = json.load(open("/tmp/wf_jobs.json"))
def t(s): return dt.datetime.fromisoformat(s.replace("Z", "+00:00"))
latest = {}
for j in jobs:                      # newest job per step
    step = j["reference"]["jobId"].rsplit("-", 1)[0]
    if step not in latest or j["status"]["stateStartTime"] > latest[step]["status"]["stateStartTime"]:
        latest[step] = j
rows = []
for step, j in sorted(latest.items()):
    state = j["status"]["state"]
    hist = j.get("statusHistory", [])
    start = next((h["stateStartTime"] for h in hist if h["state"] == "RUNNING"), None)
    end = j["status"]["stateStartTime"] if state in ("DONE", "ERROR", "CANCELLED") else None
    secs = int((t(end) - t(start)).total_seconds()) if start and end else None
    rows.append((step, state, end or "-", secs, j["placement"]["clusterName"]))
    print(f"  {step:<10} {state:<10} started {start or '-':<28} seconds {secs if secs is not None else '-'}")
    if state == "ERROR":
        print("    details:", j["status"].get("details", "")[:300])
json.dump(rows, open("/tmp/wf_rows.json", "w"))
PY

ALLDONE=$(python3 -c "import json; r=json.load(open('/tmp/wf_rows.json')); print(int(len(r)>=3 and all(x[1]=='DONE' for x in r)))")
WFCLUSTER=$(python3 -c "import json; r=json.load(open('/tmp/wf_rows.json')); print(r[0][4] if r else '')")
STILL_UP=$(gcloud dataproc clusters list --region="$REGION" --filter="clusterName ~ ^flights-wf" --format="value(clusterName)" 2>/dev/null)
if [ "$ALLDONE" = "1" ] && [ -z "$STILL_UP" ]; then
  if ! grep -q "DELETE  dataproc/${WFCLUSTER}" timestamps.txt 2>/dev/null; then
    echo "== finished: recording timings and copying check tables"
    mkdir -p results
    [ -f results/stage2_timings.csv ] || echo "utc_end,mode,job,seconds,cluster_shape" > results/stage2_timings.csv
    SHAPE="e2-standard-2 + ${WORKERS:-2} x ${WORKER_TYPE:-e2-standard-4} (workflow)"
    python3 - "$SHAPE" << 'PY' >> results/stage2_timings.csv
import json, sys
names = {"flights": "flights_to_parquet", "weather": "weather_clean", "join": "join_flights_weather"}
for step, state, end, secs, cluster in json.load(open("/tmp/wf_rows.json")):
    print(f"{end},full,{names.get(step, step)},{secs},{sys.argv[1]}")
PY
    rm -rf results/join_coverage results/source_overlap
    gcloud storage cp -r "${BUCKET}/curated/checks/*" results/ 2>/dev/null
    scripts/log_time.sh DELETE "dataproc/${WFCLUSTER} (workflow finished; cluster deleted by Dataproc)"
  fi
  echo "== timings"; column -s, -t < results/stage2_timings.csv
  echo "== coverage by year"; column -s, -t < results/join_coverage/*.csv | head -45
elif [ "$ALLDONE" != "1" ]; then
  echo "(still running or failed: run this again later)"
fi
