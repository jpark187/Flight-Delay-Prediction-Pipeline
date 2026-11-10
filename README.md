# Flight Delay Risk

Predicts the probability that a scheduled US domestic flight arrives 15+ minutes late, using only
information available before departure, for travelers booking tight connections and corporate
travel desks. Built on 38 years of BTS flight records, NOAA hourly weather and the live FAA
airport-status feed. Full plan: [docs/ROADMAP.md](docs/ROADMAP.md) · architecture: [docs/pipeline.mermaid](docs/pipeline.mermaid)

> Status: Stages 0–1 (setup and ingest). Later stages are added as they are built.

## Data sources

| Source | Obtained by | Licence |
|---|---|---|
| BTS Reporting Carrier On-Time Performance, Oct 1987 – Dec 2025 | `loaders/load_bts.sh` (BTS PREZIP; GitHub mirror fallback) | US government, public domain |
| NOAA GHCNh hourly weather (station-year Parquet) | `loaders/load_weather.sh` (NOAA AWS open-data mirror; NCEI fallback) | US government, public domain |
| OurAirports airport coordinates | `loaders/build_station_map.py` | Public domain |
| FAA NAS airport status (live XML, 1 snapshot/min) | `stream/faa_poller.sh` on a free-tier VM | US government |

No data is stored in this repo; the loaders fetch it.

## Stage 0 · Setup (once)

Prerequisites: a Google Cloud project on the free trial (do **not** upgrade the billing account),
and a budget alert (Billing → Budgets & alerts, e.g. $10). Open **Cloud Shell**, then:

```bash
git clone <this repo> && cd flight-delay-pipeline
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

### Bucket layout

```
gs://BUCKET/
  raw/YYYY/MM/*.csv                      BTS monthly files
  weather/raw/YYYY/GHCNh_<id>_YYYY.parquet  NOAA station-years (+ _COMPLETE marker per full year)
  reference/airport_station_map.csv      airport -> nearest weather station
  stream/faa/dt=YYYY-MM-DD/*.xml         FAA snapshots, one per minute
```

## Cost discipline

Every create and delete is appended to `timestamps.txt` by `scripts/log_time.sh`.
`scripts/cleanup.sh` deletes all VMs and, on confirmation, the bucket.

| Resource | Lifetime | Approx. cost |
|---|---|---|
| Bucket | until cleanup | ~$0.02 / GB-month |
| faa-poller (e2-micro, standard disk) | until the demo | VM free tier; external IP ~$3.65 / month |
| flights-loader (e2-small) | hours | cents |

## AI disclosure

See `prompts.md`.
