"""quota.py - print selected Compute Engine quotas from `gcloud ... --format=json` on stdin.
Usage: gcloud compute regions describe us-central1 --format=json | python3 scripts/quota.py CPUS,E2_CPUS
"""
import json, sys
want = set(sys.argv[1].split(","))
for x in json.load(sys.stdin)["quotas"]:
    if x["metric"] in want:
        print(f"{x['metric']:<20} used {x['usage']:>6.0f} / limit {x['limit']:.0f}")
