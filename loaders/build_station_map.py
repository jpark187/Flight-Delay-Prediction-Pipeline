"""build_station_map.py - match each airport (IATA code, as used by BTS) to its nearest
NOAA GHCNh weather station. Output: reference/airport_station_map.csv

Run from the repo root:  python3 loaders/build_station_map.py
Needs only the Python standard library.
"""
import csv, io, math, os, re, urllib.request

AIRPORTS_URL = "https://raw.githubusercontent.com/davidmegginson/ourairports-data/main/airports.csv"
STATIONS_URL = ("https://www.ncei.noaa.gov/oa/global-historical-climatology-network/"
                "hourly/doc/ghcnh-station-list.csv")
OUT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "reference", "airport_station_map.csv")
MAX_CANDIDATES = 5
MAX_KM = 15          # an airport with no station this close gets no weather (flagged, not guessed)
US_AREAS = {"US", "PR", "VI", "GU", "AS", "MP"}   # BTS covers US states and territories


def fetch_csv_rows(url):
    with urllib.request.urlopen(url, timeout=120) as r:
        return list(csv.reader(io.TextIOWrapper(r, encoding="utf-8", errors="replace")))


def haversine_km(lat1, lon1, lat2, lon2):
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp, dl = p2 - p1, math.radians(lon2 - lon1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * 6371 * math.asin(math.sqrt(a))


def load_stations():
    """Station list may or may not have a header row; handle both.
    Without a header, the first three fields are ID, latitude, longitude (GHCN convention)."""
    rows = fetch_csv_rows(STATIONS_URL)
    first = [c.strip() for c in rows[0]]
    if re.fullmatch(r"[A-Z]{2}[A-Z0-9]{9}", first[0]):
        id_i, lat_i, lon_i, name_i, body = 0, 1, 2, 5, rows   # ID, LAT, LON, ELEV, STATE, NAME
    else:
        low = [c.lower() for c in first]
        find = lambda *keys: next(i for i, c in enumerate(low) if any(k in c for k in keys))
        id_i, lat_i, lon_i = find("id"), find("lat"), find("lon")
        name_i = next((i for i, c in enumerate(low) if "name" in c), None)
        body = rows[1:]
    stations = []
    for r in body:
        try:
            sid = r[id_i].strip()
            if not sid.startswith(("US", "RQ", "VQ", "GQ", "AQ", "CQ")):   # US + territories
                continue
            stations.append((sid, float(r[lat_i]), float(r[lon_i]),
                             r[name_i].strip() if name_i is not None and len(r) > name_i else ""))
        except (ValueError, IndexError):
            continue
    print(f"loaded {len(stations):,} US-area stations; header row: {first[:6]}")
    return stations


def load_airports():
    rows = fetch_csv_rows(AIRPORTS_URL)
    hdr = rows[0]
    out = []
    for r in rows[1:]:
        a = dict(zip(hdr, r))
        if a["iata_code"] and a["iso_country"] in US_AREAS and a["scheduled_service"] == "yes":
            out.append((a["iata_code"], float(a["latitude_deg"]), float(a["longitude_deg"]), a["name"]))
    print(f"loaded {len(out):,} US airports with scheduled service")
    return out


def main():
    stations, airports = load_stations(), load_airports()
    matched = 0
    with open(OUT, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["iata", "airport_name", "ghcn_id", "station_name", "distance_km", "candidates"])
        for iata, alat, alon, aname in airports:
            # All stations within MAX_KM, airport (USW) stations first, then by distance.
            # Stations open, close and get renumbered over 38 years, so the loader tries
            # these in order for each year and records which one it actually used.
            near = sorted(
                ((haversine_km(alat, alon, s[1], s[2]), s) for s in stations),
                key=lambda x: x[0])
            near = [(d, s) for d, s in near if d <= MAX_KM]
            near.sort(key=lambda x: (not x[1][0].startswith("USW"), x[0]))
            near = near[:MAX_CANDIDATES]
            if near:
                matched += 1
                d, best = near[0]
                w.writerow([iata, aname, best[0], best[3], f"{d:.2f}",
                            ";".join(s[0] for _, s in near)])
            else:
                w.writerow([iata, aname, "", "", "", ""])
    print(f"matched {matched}/{len(airports)} airports within {MAX_KM} km -> reference/airport_station_map.csv")


if __name__ == "__main__":
    main()
