"""ask_it_twice.py - one key number computed three independent ways, which must agree:
ORD's 2024 late rate (operated flights with a known outcome, late = arrived 15+ min late).

  1. Spark SQL over the curated Parquet history      (written by spark/build_serving_docs.py)
  2. DuckDB SQL directly on the 2024 Parquet files   (no Spark involved)
  3. MongoDB aggregation (MQL) over route_months     (sums route-by-month documents in Atlas)

    source env.sh && python3 scripts/ask_it_twice.py
Writes results/ask_it_twice.csv.
"""
import csv, glob, io, os, subprocess, sys

ORIGIN, YEAR = os.environ.get("ASK_ORIGIN", "ORD"), int(os.environ.get("ASK_YEAR", "2024"))
MQL = [{"$match": {"origin": ORIGIN, "year": YEAR}},
       {"$group": {"_id": None, "flights": {"$sum": "$n"}, "late": {"$sum": "$late"}}}]

def spark_value(bucket):
    out = subprocess.run(["gcloud", "storage", "cat", f"{bucket}/curated/checks/ask_twice_spark/part-*.csv"],
                         check=True, capture_output=True, text=True).stdout
    r = next(csv.DictReader(io.StringIO(out)))
    return int(r["flights"]), int(float(r["late"]))

def duckdb_value(parquet_dir):
    import duckdb
    n, late = duckdb.connect().execute(f"""
        SELECT count(*), sum(arr_del15)::BIGINT
        FROM read_parquet('{parquet_dir}/**/*.parquet', hive_partitioning = 1)
        WHERE origin = '{ORIGIN}' AND year = {YEAR} AND cancelled = 0 AND arr_del15 IS NOT NULL
    """).fetchone()
    return int(n), int(late)

def mongo_value(db):
    r = list(db.route_months.aggregate(MQL))[0]
    return int(r["flights"]), int(r["late"])

def report(rows):
    print(f"\n{ORIGIN} {YEAR} late rate, three ways:")
    for m, n, late in rows:
        print(f"  {m:<11} flights {n:>9,}  late {late:>8,}  rate {late/n:.6f}")
    agree = len({(n, late) for _, n, late in rows}) == 1
    print("  AGREE: identical counts from all three methods" if agree else "  DISAGREE: investigate")
    os.makedirs("results", exist_ok=True)
    with open("results/ask_it_twice.csv", "w", newline="") as fh:
        w = csv.writer(fh); w.writerow(["method", "origin", "year", "flights", "late", "late_rate"])
        for m, n, late in rows:
            w.writerow([m, ORIGIN, YEAR, n, late, f"{late/n:.6f}"])
    print("  MQL used:", MQL)
    return agree

def main():
    bucket, uri = os.environ.get("BUCKET"), os.environ.get("MONGO_URI")
    if not bucket or not uri:
        sys.exit("source env.sh first (BUCKET and MONGO_URI)")
    local = os.path.expanduser(f"~/data/flights_{YEAR}")
    if not glob.glob(f"{local}/**/*.parquet", recursive=True):
        print(f"downloading curated/flights/year={YEAR} for DuckDB...")
        os.makedirs(local, exist_ok=True)
        subprocess.run(["gcloud", "storage", "cp", "-r", f"{bucket}/curated/flights/year={YEAR}", local + "/"],
                       check=True, capture_output=True)
    from pymongo import MongoClient
    db = MongoClient(uri, serverSelectionTimeoutMS=20000)[os.environ.get("MONGO_DB", "flights")]
    rows = [("spark_sql", *spark_value(bucket)), ("duckdb_sql", *duckdb_value(local)), ("mongodb_mql", *mongo_value(db))]
    sys.exit(0 if report(rows) else 1)

if __name__ == "__main__":
    main()
