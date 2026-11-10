"""build_airport_tz.py - IATA airport code -> IANA time zone (e.g. ORD -> America/Chicago).

BTS scheduled times (CRSDepTime) are LOCAL clock times; NOAA weather times are UTC.
Joining them needs each airport's time zone, including daylight-saving rules, so Spark can
convert scheduled departures to UTC.

Sources: the `airportsdata` package (MIT licence) for time zones, plus OurAirports for codes
airportsdata does not carry under the code BTS uses. Example: BTS reports West Palm Beach as
PBI, while airportsdata and OurAirports now list it as DJT (its ident is still KPBI).
  * codes missing from airportsdata get the time zone of the nearest airportsdata airport
    within 25 km (tz_source=nearest)
  * an older code recoverable from the OurAirports ident (KPBI -> PBI) is added as an alias with
    the airport's time zone (tz_source=alias), and written to reference/airport_alias.csv
    (code,ref_code) so Spark can find the weather station recorded under the current code.

Run from the repo root:  pip install --user airportsdata && python3 loaders/build_airport_tz.py
Output: reference/airport_tz.csv, reference/airport_alias.csv
"""
import csv, io, math, os, urllib.request
import airportsdata

REF = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "reference")
OUT = os.path.join(REF, "airport_tz.csv")
ALIAS_OUT = os.path.join(REF, "airport_alias.csv")
OURAIRPORTS = "https://raw.githubusercontent.com/davidmegginson/ourairports-data/main/airports.csv"
MAX_KM = 25


def km(lat1, lon1, lat2, lon2):
    p1, p2 = math.radians(lat1), math.radians(lat2)
    a = (math.sin((p2 - p1) / 2) ** 2
         + math.cos(p1) * math.cos(p2) * math.sin(math.radians(lon2 - lon1) / 2) ** 2)
    return 2 * 6371 * math.asin(math.sqrt(a))


rows = {}
known = []   # (lat, lon, tz) for the nearest-airport fallback
for code, a in airportsdata.load("IATA").items():
    if a.get("tz"):
        rows[code] = [code, a["tz"], a["country"], a["name"], a["lat"], a["lon"], "airportsdata"]
for a in airportsdata.load("ICAO").values():
    if a.get("tz") and a.get("lat") is not None:
        known.append((float(a["lat"]), float(a["lon"]), a["tz"]))

added, aliases = [], []
with urllib.request.urlopen(OURAIRPORTS, timeout=120) as r:
    ourairports = [a for a in csv.DictReader(io.TextIOWrapper(r, encoding="utf-8"))
                   if a["iata_code"] and a["type"] in ("large_airport", "medium_airport", "small_airport")]
for a in ourairports:
    code = a["iata_code"]
    if code in rows:
        continue
    lat, lon = float(a["latitude_deg"]), float(a["longitude_deg"])
    d, tz = min(((km(lat, lon, k[0], k[1]), k[2]) for k in known), key=lambda x: x[0])
    if d <= MAX_KM:
        rows[code] = [code, tz, a["iso_country"], a["name"], lat, lon, "nearest"]
        added.append(code)
for a in ourairports:   # older codes: US ident K + 3 letters that differs from the current IATA
    ident, code = a["ident"], a["iata_code"]
    old_code = ident[1:] if len(ident) == 4 and ident.startswith("K") and ident[1:].isalpha() else None
    if old_code and old_code != code and old_code not in rows and code in rows:
        r0 = rows[code]
        rows[old_code] = [old_code, r0[1], r0[2], r0[3], r0[4], r0[5], "alias"]
        aliases.append((old_code, code))
with open(ALIAS_OUT, "w", newline="") as f:
    w = csv.writer(f)
    w.writerow(["code", "ref_code"])
    w.writerows(sorted(aliases))

with open(OUT, "w", newline="") as f:
    w = csv.writer(f)
    w.writerow(["iata", "tz", "country", "name", "lat", "lon", "tz_source"])
    w.writerows(rows[c] for c in sorted(rows))
print(f"wrote {len(rows):,} airports ({len(added)} via nearest-airport fallback, "
      f"{len(aliases)} older-code aliases e.g. {aliases[:5]}) -> reference/airport_tz.csv, airport_alias.csv")
