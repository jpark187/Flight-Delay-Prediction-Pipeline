"""load_mongo.py - load the serving documents built by spark/build_serving_docs.py into MongoDB
Atlas, create indexes, and report what was loaded.

    source env.sh && python3 scripts/load_mongo.py
Needs MONGO_URI in env.sh (never in the repo). Database: $MONGO_DB (default 'flights').
Collections are replaced on each run, so rerunning is safe.
"""
import glob, json, os, subprocess, sys, tempfile

COLLECTIONS = ["airports", "carriers", "routes", "route_months"]
INDEXES = {
    "routes": [[("origin", 1)], [("dest", 1)]],
    "route_months": [[("origin", 1), ("year", 1)], [("origin", 1), ("dest", 1), ("year", 1), ("month", 1)]],
}

def download(bucket, workdir):
    for c in COLLECTIONS:
        os.makedirs(f"{workdir}/{c}", exist_ok=True)
        subprocess.run(["gcloud", "storage", "cp", f"{bucket}/serving/{c}/part-*.json", f"{workdir}/{c}/"],
                       check=True, capture_output=True)

def load(db, workdir, batch=5000):
    report = {}
    for c in COLLECTIONS:
        docs = [json.loads(line) for f in sorted(glob.glob(f"{workdir}/{c}/*.json")) for line in open(f)]
        db[c].drop()
        for i in range(0, len(docs), batch):
            db[c].insert_many(docs[i:i + batch], ordered=False)
        for keys in INDEXES.get(c, []):
            db[c].create_index(keys)
        report[c] = db[c].count_documents({})
        print(f"  {c:<13} {report[c]:>9,} documents")
    return report

def main():
    uri, bucket = os.environ.get("MONGO_URI"), os.environ.get("BUCKET")
    if not uri or not bucket:
        sys.exit("set MONGO_URI and BUCKET in env.sh, then: source env.sh")
    from pymongo import MongoClient
    db = MongoClient(uri, serverSelectionTimeoutMS=20000)[os.environ.get("MONGO_DB", "flights")]
    db.command("ping")
    print("connected to MongoDB (credentials not shown)")
    with tempfile.TemporaryDirectory() as tmp:
        print("downloading serving documents from GCS..."); download(bucket, tmp)
        print("loading:"); load(db, tmp)
    st = db.command("dbstats")
    print(f"database size: data {st['dataSize']/1e6:.1f} MB, storage {st['storageSize']/1e6:.1f} MB, "
          f"indexes {st['indexSize']/1e6:.1f} MB (Atlas M0 limit: 512 MB)")

if __name__ == "__main__":
    main()
