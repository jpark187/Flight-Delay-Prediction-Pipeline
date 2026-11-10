"""build_airport_tz.py - IATA airport code -> IANA time zone (e.g. ORD -> America/Chicago).

BTS scheduled times (CRSDepTime) are LOCAL clock times; NOAA weather times are UTC.
Joining them needs each airport's time zone, including daylight-saving rules, so Spark can
convert scheduled departures to UTC. Source: the `airportsdata` package (MIT licence).

Run from the repo root:  pip install --user airportsdata && python3 loaders/build_airport_tz.py
Output: reference/airport_tz.csv
"""
import csv, os
import airportsdata

OUT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "reference", "airport_tz.csv")
airports = airportsdata.load("IATA")
with open(OUT, "w", newline="") as f:
    w = csv.writer(f)
    w.writerow(["iata", "tz", "country", "name", "lat", "lon"])
    for code, a in sorted(airports.items()):
        if a.get("tz"):
            w.writerow([code, a["tz"], a["country"], a["name"], a["lat"], a["lon"]])
print(f"wrote {len(airports):,} airports -> reference/airport_tz.csv")
