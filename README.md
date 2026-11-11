# Flight Delay Risk

Predicts the probability that a scheduled US domestic flight arrives 15+ minutes late, using only
information available before departure, for travelers booking tight connections and corporate
travel desks. Built on 38 years of US flight records (BTS, with the 1990s filled from the ASA Data Expo),
NOAA hourly weather and the live FAA airport-status feed. Full plan: [docs/ROADMAP.md](docs/ROADMAP.md) · architecture: [docs/pipeline.mermaid](docs/pipeline.mermaid)

> Status: Stages 0–2 (setup, ingest, Spark transform). Later stages are added as they are built.

## Data sources

| Source | Obtained by | Licence |
|---|---|---|
| BTS Reporting Carrier On-Time Performance, Oct 1987 – Dec 2025 | `loaders/load_bts.sh` (BTS PREZIP; GitHub mirror fallback) | US government, public domain |
| ASA Data Expo 2009 (BTS data, Oct 1987 – Apr 2008), used for 1990–1999, which BTS PREZIP no longer serves (404 for every month, checked Nov 10, 2026) | `loaders/load_expo.sh` (Harvard Dataverse, doi:10.7910/DVN/HG7NV7) | Public research dataset |
| NOAA GHCNh hourly weather (station-year Parquet) | `loaders/load_weather.sh` (NOAA AWS open-data mirror; NCEI fallback) | US government, public domain |
| OurAirports airport coordinates | `loaders/build_station_map.py` | Public domain |
| Airport time zones (`airportsdata` package) | `loaders/build_airport_tz.py` | MIT licence |
| FAA NAS airport status (live XML, 1 snapshot/min) | `stream/faa_poller.sh` on a free-tier VM | US government |

No data is stored in this repo; the loaders fetch it.

## Stage 0 · Setup (once)

Prerequisites: a Google Cloud project on the free trial (do **not** upgrade the billing account),
and a budget alert (Billing → Budgets & alerts, e.g. $10). Open **Cloud Shell**, then:

```bash
git clone https://github.com/jpark187/Flight-Delay-Prediction-Pipeline.git flight-delay-pipeline
cd flight-delay-pipeline
cp env.sh.example env.sh        # edit: PROJECT_ID, REGION, ZONE, BUCKET
scripts/setup.sh                # enables APIs, creates the bucket
```

Choose a bucket name that does not contain your project id. `env.sh` is git-ignored; never commit it.

## Stage 1 · Ingest

**1. Sample run (Cloud Shell, a few minutes).** 2024 flights plus 2024 weather for the 30 busiest airports. This is also the grader path.

```bash
scripts/run_stage1.sh sample
```

Expected: 12 monthly flight files, ~30 weather files, a matched-airport count, bytes per folder.
`build_station_map.py` also prints the NOAA station-list header and the match rate; check both.

**2. Start the FAA poller (runs until the demo).**

```bash
scripts/create_poller_vm.sh
scripts/check_poller.sh         # after ~3 minutes: count of today's snapshots + the latest one
```

**3. Full run (loader VM, several hours).**

```bash
scripts/create_loader_vm.sh     # prints the copy, ssh and tmux commands to run next
# ... when the full run has finished:
scripts/delete_loader_vm.sh
```

Both loaders are resumable: rerun the same command after any failure and finished months/years are skipped.

**4. Fill the 1990s gap (any machine, ~10 minutes).**

```bash
START_YEAR=1990 END_YEAR=1999 loaders/load_expo.sh
```

## Stage 2 · Transform (Spark on Dataproc)

```bash
scripts/check_quota.sh          # CPU quota caps the cluster size
scripts/run_stage2.sh sample    # 2024 only; single-node cluster, created and deleted by the script
scripts/run_stage2.sh full      # all years; WORKERS=2 WORKER_TYPE=e2-standard-4 by default
# or, for the long run, as a Dataproc workflow that needs no open terminal:
scripts/run_stage2_workflow.sh full && scripts/stage2_status.sh   # rerun status any time
```

Three jobs in `spark/`: `flights_to_parquet.py` (BTS + Data Expo → one schema, with a
month-by-month source comparison), `weather_clean.py` (one quality-checked report per
station-hour), `join_flights_weather.py` (scheduled departure converted to UTC with the
airport's time zone; latest weather report taken before departure at origin and destination).
Job run times are appended to `results/stage2_timings.csv`; check tables are copied to `results/`.

## Stage 3 · EDA (DuckDB, no cluster)

```bash
pip install --user duckdb pandas matplotlib
source env.sh && python3 eda/eda_2024.py      # charts + summary in results/eda/
```

Full history (230M flights) is too big for Cloud Shell, so Spark computes small aggregate tables
on a cluster and the charts are drawn locally from those:

```bash
WORKERS=3 scripts/run_eda_workflow.sh       # Dataproc workflow, ~30-45 min, cluster auto-deleted
WORKERS=3 scripts/stage2_status.sh           # until DONE
source env.sh && python3 eda/eda_full.py     # charts + summary in results/eda_full/
```

## Stage 4 · Features (Spark on Dataproc)

`spark/build_features.py` builds one row per operated flight for 2018–2025 using only information
available before departure: schedule, rolling late rates over the days *before* the flight
(route, origin, origin × hour, destination, carrier; sparse routes shrunk toward the origin),
aircraft rotation from the schedule, and decoded origin/destination weather. Split: train 2018,
2019, 2021–2023; 2020 flagged; valid 2024; test 2025.

```bash
WORKERS=3 scripts/run_workflow.sh features build_features    # no open terminal needed
WORKERS=3 scripts/stage2_status.sh                            # until DONE
```

### Bucket layout

```
gs://BUCKET/
  raw/YYYY/MM/*.csv                      BTS monthly files
  weather/raw/YYYY/GHCNh_<id>_YYYY.parquet  NOAA station-years (+ _COMPLETE marker per full year)
  reference/airport_station_map.csv      airport -> nearest weather station
  raw_expo/YYYY.csv.bz2                  Data Expo years (1990-1999)
  weather/manifest/YYYY.csv              airport -> station actually used each year
  reference/airport_tz.csv               airport -> IANA time zone
  stream/faa/dt=YYYY-MM-DD/*.xml         FAA snapshots, one per minute
  curated/flights/  curated/weather_hourly/  curated/flights_wx/   Spark outputs (Parquet)
  curated/checks/                        source overlap, join coverage
```

## Cost discipline

Every create and delete is appended to `timestamps.txt` by `scripts/log_time.sh`.
`scripts/cleanup.sh` deletes all VMs and, on confirmation, the bucket.

| Resource | Lifetime | Approx. cost |
|---|---|---|
| Bucket | until cleanup | ~$0.02 / GB-month |
| faa-poller (e2-micro, standard disk) | until the demo | VM free tier; external IP ~$3.65 / month |
| flights-loader (e2-small) | hours | cents |
| flights-spark (Dataproc) | per run; deleted by the script, auto-deletes after 30 idle min | cents to dollars per run |

## AI disclosure

See `prompts.md`.
