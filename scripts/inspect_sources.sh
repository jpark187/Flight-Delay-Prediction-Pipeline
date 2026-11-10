#!/usr/bin/env bash
# inspect_sources.sh - print the real columns of every source, so Stage 2 Spark code is written
# against actual schemas, plus the CPU quota that limits the Dataproc cluster size.
# Run in Cloud Shell from the repo root. Installs duckdb (also used later for EDA).
set -uo pipefail
cd "$(dirname "$0")/.."
source env.sh
pip install --user --quiet duckdb pandas airportsdata 2>/dev/null
T=$(mktemp -d); trap 'rm -rf "$T"' EXIT

echo "################ BTS flights (2024-01): header + 2 rows"
f=$(gcloud storage ls "${BUCKET}/raw/2024/01/*.csv" | head -1)
gcloud storage cat -r 0-200000 "$f" > "$T/bts.csv" 2>/dev/null
head -3 "$T/bts.csv"
echo; echo "column count: $(head -1 "$T/bts.csv" | tr ',' '\n' | wc -l)"

echo; echo "################ Data Expo (1995): header + 2 rows"
gcloud storage cat "${BUCKET}/raw_expo/1995.csv.bz2" 2>/dev/null | bzcat 2>/dev/null | head -3

echo; echo "################ NOAA GHCNh weather (ORD 2024): schema + sample"
w=$(gcloud storage ls "${BUCKET}/weather/raw/2024/GHCNh_USW00094846_2024.parquet" 2>/dev/null || \
    gcloud storage ls "${BUCKET}/weather/raw/2024/*.parquet" | head -1)
gcloud storage cp -q "$w" "$T/w.parquet"
python3 - "$T/w.parquet" << 'PY'
import duckdb, sys
p = sys.argv[1]
con = duckdb.connect()
cols = con.execute(f"DESCRIBE SELECT * FROM '{p}'").fetchall()
print(f"{len(cols)} columns:")
print(", ".join(f"{c[0]}:{c[1]}" for c in cols))
print("\nrows:", con.execute(f"SELECT count(*) FROM '{p}'").fetchone()[0])
key = [c[0] for c in cols if c[0].split('_')[0] in
       ("Station", "DATE", "temperature", "wind", "visibility", "precipitation", "pres")
       or c[0] in ("DATE",)][:24]
print("\nsample of key columns:")
print(con.execute(f"SELECT {', '.join(key)} FROM '{p}' LIMIT 5").df().to_string())
for c in [c[0] for c in cols if c[0].endswith("Report_Type")][:1]:
    print(f"\nreport types ({c}):")
    print(con.execute(f"SELECT {c}, count(*) n FROM '{p}' GROUP BY 1 ORDER BY n DESC").df().to_string())
PY

echo; echo "################ CPU quota (limits Dataproc cluster size)"
gcloud compute regions describe "$REGION" --format=json \
  | python3 scripts/quota.py CPUS,E2_CPUS,N2_CPUS,SSD_TOTAL_GB,DISKS_TOTAL_GB,IN_USE_ADDRESSES
gcloud compute project-info describe --format=json | python3 scripts/quota.py CPUS_ALL_REGIONS
