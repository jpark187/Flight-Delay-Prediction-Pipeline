#!/usr/bin/env bash
# check_quota.sh - CPU quota (usage / limit) that caps the Dataproc cluster size.
cd "$(dirname "$0")/.."; source env.sh
show() {
python3 -c '
import json, sys
want = set(sys.argv[1:])
for q in json.load(sys.stdin)["quotas"]:
    if q["metric"] in want:
        print("%-18s used %6g of %6g" % (q["metric"], q["usage"], q["limit"]))
' "$@"
}
gcloud compute regions describe "$REGION" --format=json | show CPUS E2_CPUS N2_CPUS DISKS_TOTAL_GB IN_USE_ADDRESSES
gcloud compute project-info describe --format=json | show CPUS_ALL_REGIONS
