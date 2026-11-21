"""Stage 6 - Spark Structured Streaming over the FAA airport-status feed.

The poller (stream/faa_poller.sh) lands one XML snapshot per minute in
{base}/stream/faa/dt=YYYY-MM-DD/YYYYMMDDTHHMMSSZ.xml. This job reads new snapshots as they arrive
(file stream source, checkpointed, so each snapshot is processed exactly once), parses every
airport event (ground stops, ground delay programs, closures, departure/arrival delays), and
compares each snapshot with the previous one to find when events START and END.

MongoDB (connection string read at runtime from Secret Manager secret 'mongo-uri'):
  airport_status   one document per airport with an active event: what, why, how long, since when
  faa_events       history: one document per event with start, end, peak delay, snapshots seen
  faa_snapshots    per-snapshot counts of active events by type (disruption over time)
  faa_state        the active set after the last processed snapshot (lets the next run continue)

Modes
  --mode full    availableNow: process everything not yet processed (the backlog), then stop
  --mode live    keep running for 10 minutes, checking for new snapshots every 30 s (the demo)
"""
import os, re, subprocess, time
import xml.etree.ElementTree as ET
from pyspark.sql import functions as F
from common import parse_args, spark_session

args = parse_args(__doc__)
spark = spark_session("stream_faa")
base = args.base
LIVE_MINUTES = int(os.environ.get("LIVE_MINUTES", "10" if args.mode == "live" else "0"))
DB_NAME = os.environ.get("MONGO_DB", "flights")

# ---------------------------------------------------------------- MongoDB connection (driver only)
def mongo_db():
    if os.environ.get("MONGO_TEST") == "mongomock":                  # local tests only
        import mongomock, sys
        if not hasattr(sys, "_faa_mock"):
            sys._faa_mock = mongomock.MongoClient()
        return sys._faa_mock[DB_NAME]
    uri = subprocess.run(["gcloud", "secrets", "versions", "access", "latest", "--secret=mongo-uri"],
                         check=True, capture_output=True, text=True).stdout.strip()
    try:
        from pymongo import MongoClient
    except ImportError:                    # only the driver talks to MongoDB, so install it here
        import sys
        subprocess.run([sys.executable, "-m", "pip", "install", "--quiet", "pymongo==4.10.1"], check=True)
        from pymongo import MongoClient
    return MongoClient(uri, serverSelectionTimeoutMS=20000)[DB_NAME]

# ---------------------------------------------------------------- parsing
def minutes(text):
    """'1 hour and 26 minutes' -> 86; '45 minutes' -> 45; '2 hours' -> 120; otherwise None."""
    if not text:
        return None
    h = re.search(r"(\d+)\s*hour", text); m = re.search(r"(\d+)\s*min", text)
    return (int(h.group(1)) * 60 if h else 0) + (int(m.group(1)) if m else 0) if (h or m) else None

TYPE = {"ground delay programs": "ground_delay_program", "ground stops": "ground_stop",
        "ground stop programs": "ground_stop",          # the feed's actual section name
        "airport closures": "closure", "general arrival/departure delay info": "delay"}

def parse_snapshot(xml_text):
    """-> {event_key: details} for every airport event in one snapshot."""
    out = {}
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError:
        return None
    for dt in root.iter("Delay_type"):
        name = (dt.findtext("Name") or "").strip()
        etype = TYPE.get(name.lower(), re.sub(r"\W+", "_", name.lower()).strip("_") or "other")
        for rec in dt.iter():
            arpt = rec.findtext("ARPT")
            if not arpt or rec.find("ARPT") is None or rec.tag == "Delay_type":
                continue
            parts = rec.findall("Arrival_Departure") or [None]
            for p in parts:
                sub = p.get("Type", "").lower() if p is not None else ""
                src = p if p is not None else rec
                d = {"airport": arpt.strip(), "type": etype, "direction": sub or None,
                     "reason": (rec.findtext("Reason") or "").strip() or None,
                     "avg_min": minutes(src.findtext("Avg")), "max_min": minutes(src.findtext("Max")),
                     "min_min": minutes(src.findtext("Min")), "trend": src.findtext("Trend"),
                     "end_time_text": rec.findtext("End_Time") or rec.findtext("Reopen")}
                out[f"{d['airport']}|{etype}|{sub}"] = d
    return out

def snapshot_ts(path):
    m = re.search(r"(\d{8}T\d{6}Z)\.xml", path)
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.strptime(m.group(1), "%Y%m%dT%H%M%SZ")) if m else None

def write_ops(coll, ops):
    """apply (filter, update, upsert) in order: one bulk round trip to Atlas; one by one in tests."""
    if not ops:
        return
    if os.environ.get("MONGO_TEST") == "mongomock":
        for f, u, up in ops:
            coll.update_one(f, u, upsert=up)
        return
    from pymongo import UpdateOne
    coll.bulk_write([UpdateOne(f, u, upsert=up) for f, u, up in ops], ordered=True)

# ---------------------------------------------------------------- per micro-batch: events + status
def process(batch_df, batch_id):
    rows = batch_df.select("path", "value").collect()
    snaps = sorted(((snapshot_ts(r.path), r.value) for r in rows if snapshot_ts(r.path)), key=lambda x: x[0])
    if not snaps:
        return
    db = mongo_db()
    ops = []                      # (filter, update, upsert): one ordered bulk write per batch
    state = db.faa_state.find_one({"_id": "active"}) or {"_id": "active", "events": {}, "last_ts": None}
    active = state["events"]                          # key -> {event_id, start, details...}
    counts, bad = [], 0
    for ts, text in snaps:
        if state["last_ts"] and ts <= state["last_ts"]:
            continue                                  # already applied (restart safety)
        cur = parse_snapshot(text)
        if cur is None:
            bad += 1; continue
        for key in list(active):                      # ended: active before, gone now
            if key not in cur:
                ev = active.pop(key)
                ops.append(({"_id": ev["event_id"]}, {"$set": {"end": ts, "active": False}}, False))
        for key, d in cur.items():
            if key not in active:                     # started
                eid = f"{key}|{ts}"
                active[key] = {"event_id": eid, "start": ts}
                ops.append(({"_id": eid}, {"$setOnInsert": {**d, "start": ts}, "$set": {
                    "active": True, "last_seen": ts, "peak_avg_min": d["avg_min"] or 0,
                    "peak_max_min": d["max_min"] or 0}, "$inc": {"snapshots": 1}}, True))
            else:                                     # continuing
                ops.append(({"_id": active[key]["event_id"]},
                    {"$set": {"last_seen": ts, "trend": d["trend"], "end_time_text": d["end_time_text"]},
                     "$max": {"peak_avg_min": d["avg_min"] or 0, "peak_max_min": d["max_min"] or 0},
                     "$inc": {"snapshots": 1}}, False))
            active[key].update({k: v for k, v in d.items()})
        by_type = {}
        for d in cur.values():
            by_type[d["type"]] = by_type.get(d["type"], 0) + 1
        counts.append({"_id": ts, "ts": ts, "active": len(cur), "by_type": by_type})
        state["last_ts"] = ts
    write_ops(db.faa_events, ops)
    write_ops(db.faa_snapshots, [({"_id": c["_id"]}, {"$set": {k: v for k, v in c.items() if k != "_id"}}, True)
                                 for c in counts])
    state["events"] = active
    db.faa_state.replace_one({"_id": "active"}, state, upsert=True)
    # current status: one document per airport with something active
    by_airport = {}
    for key, ev in active.items():
        by_airport.setdefault(ev["airport"], []).append(
            {k: ev.get(k) for k in ("type", "direction", "reason", "avg_min", "max_min", "min_min", "trend",
                                    "end_time_text", "start")})
    db.airport_status.delete_many({})
    if by_airport:
        db.airport_status.insert_many([{"_id": a, "airport": a, "as_of": state["last_ts"], "events": evs}
                                       for a, evs in by_airport.items()])
    print(f"batch {batch_id}: {len(snaps)} snapshots up to {state['last_ts']}, "
          f"{len(active)} active events at {len(by_airport)} airports, unparsable {bad}")

# ---------------------------------------------------------------- the stream
src = (spark.readStream.option("wholetext", True).option("maxFilesPerTrigger", 2000)
       .text(f"{base}/stream/faa/*/").withColumn("path", F.input_file_name()))
writer = (src.writeStream.foreachBatch(process)
          .option("checkpointLocation", f"{base}/stream/checkpoints/faa"))
if LIVE_MINUTES > 0:
    q = writer.trigger(processingTime="30 seconds").start()
    q.awaitTermination(LIVE_MINUTES * 60); q.stop()
else:
    q = writer.trigger(availableNow=True).start()
    q.awaitTermination()

# ---------------------------------------------------------------- summary for results/
db = mongo_db()
ev = list(db.faa_events.find({}, {"airport": 1, "type": 1, "start": 1, "end": 1, "peak_avg_min": 1}))
snaps = db.faa_snapshots.count_documents({})
summ = {}
for e in ev:
    s = summ.setdefault(e["type"], {"events": 0, "airports": set()})
    s["events"] += 1; s["airports"].add(e["airport"])
rows = [(t, v["events"], len(v["airports"]), snaps) for t, v in sorted(summ.items())]
spark.createDataFrame(rows or [("none", 0, 0, snaps)], ["event_type", "events", "airports", "snapshots_processed"]) \
     .coalesce(1).write.mode("overwrite").option("header", True).csv(f"{base}/curated/checks/faa_stream_summary")
print(f"snapshots processed: {snaps:,}; events: {len(ev):,};", {t: v['events'] for t, v in summ.items()})
