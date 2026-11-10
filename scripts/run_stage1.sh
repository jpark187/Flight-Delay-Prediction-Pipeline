#!/usr/bin/env bash
# run_stage1.sh sample|full - load all raw data into GCS.
#   sample: 2024 flights + 2024 weather for the 30 busiest airports (minutes; Cloud Shell; grader path)
#   full:   Oct 1987 - Dec 2025 flights + weather for every matched airport (hours; loader VM in tmux)
set -euo pipefail
cd "$(dirname "$0")/.."
source env.sh
MODE=${1:-sample}
TOP30="ATL,DFW,DEN,ORD,LAX,JFK,CLT,LAS,MCO,MIA,PHX,SEA,SFO,EWR,IAH,BOS,FLL,MSP,LGA,DTW,PHL,SLC,DCA,SAN,BWI,TPA,AUS,IAD,BNA,MDW"

echo "== 1/3 reference tables: airport -> weather station, airport -> time zone"
python3 loaders/build_station_map.py
pip install --user --quiet airportsdata 2>/dev/null || pip install --quiet airportsdata --break-system-packages
python3 loaders/build_airport_tz.py
gcloud storage cp reference/airport_station_map.csv reference/airport_tz.csv "${BUCKET}/reference/"

case "$MODE" in
  sample)
    echo "== 2/3 flights (2024)"
    START_YEAR=2024 END_YEAR=2024 loaders/load_bts.sh
    echo "== 3/3 weather (2024, 30 airports)"
    START_YEAR=2024 END_YEAR=2024 AIRPORTS="$TOP30" loaders/load_weather.sh ;;
  full)
    echo "== 2/3 flights (Oct 1987 - Dec 2025)"
    loaders/load_bts.sh
    echo "== 3/3 weather (1987-2025, all matched airports)"
    loaders/load_weather.sh ;;
  *) echo "usage: $0 sample|full"; exit 1 ;;
esac
scripts/verify_stage1.sh
