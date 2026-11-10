# Flight Delay Risk: Project Roadmap

ADSP 32030 · Final project · Final submission Friday, December 11, 11:59 PM CT

This roadmap is written to satisfy both requirement documents received so far: the course PDF that the proposal was approved against (100 points, three platforms from three weeks, one measured scaling decision) and the 200-point rubric (EDA, queries, class-tool list, findings). Where they differ, the plan does both. One open item: confirm with Jan which document governs grading.

## 1. The problem

Travelers booking tight connections and corporate travel desks choosing between itineraries have no easy way to know, at booking time, how likely a specific flight is to arrive late. The project predicts the probability that a scheduled US domestic flight arrives 15 or more minutes late, using only information available before departure, and explains that risk with the route's history, weather, and live airport conditions. A good answer changes a decision: picking a different flight, adding buffer before a connection, or rebooking ahead of a storm.

The pre-departure rule is the backbone of the design. The model never sees departure delay, taxi times, actual times, or weather after scheduled departure. Training uses weather from the hour before scheduled departure; serving uses the latest observation at request time.

## 2. Data sources

| Source | What it is | Size / rate | How obtained | Right to use | Hard parts |
|---|---|---|---|---|---|
| BTS Reporting Carrier On-Time Performance | Every US domestic flight by reporting carriers | Oct 1987 – Dec 2025, 459 monthly files, ~200M+ rows, ~100 GB CSV (confirm after load) | `load_bts.sh` from BTS PREZIP, GitHub mirror as fallback | US government, public domain | Slow server; columns that are empty in early eras; cancelled and diverted flights; schema drift |
| NOAA GHCNh hourly weather | Hourly observations at weather stations | One Parquet file per station-year, ~700 airports × 39 years | `load_weather.sh` from NOAA's AWS mirror, NCEI fallback | US government, public domain | 230+ columns; quality flags; multiple report types per hour; units; gaps |
| OurAirports | Airport coordinates by IATA code | ~87k airports, ~700 US with scheduled service | `build_station_map.py` | Public domain | Matching airports to the nearest station; airports with no station nearby |
| FAA NAS airport status | Live ground stops, ground delay programs, closures | One XML snapshot per minute | `faa_poller` on a free-tier e2-micro VM | US government | Live only, no history; XML; events start and end |

Big-data case: the full flight history plus weather is far beyond a laptop's memory (and roughly at the limit of its disk), and the FAA feed adds a true streaming source. Both requirement documents' rules are met by either one; together they are safe.

## 3. Tools and what each does

| Tool | Job in the pipeline | Counts toward |
|---|---|---|
| Google Cloud Storage | Data lake: raw files, curated Parquet, stream landing zone, model files | PDF Week 1 |
| Spark on Dataproc | Clean and convert, weather QC, flight × weather join, window features, model training, Structured Streaming over the FAA feed | PDF Week 2 · 200-pt list (Spark) |
| MongoDB Atlas (free M0) | Route documents, airport documents, live airport status. Read on every API request and by the MCP server | PDF Week 4 · 200-pt list |
| Claude + MongoDB MCP server | Answers plain-English questions over the route and airport documents; the AI demo | PDF Week 10 · 200-pt list |
| DuckDB | EDA directly on Parquet; independent recomputation of key numbers | 200-pt list |
| Cloud Run | Prediction API, autoscaled; target of the min-instances test | PDF Week 3 |
| Streamlit | Findings dashboard | Presentation |
| Airflow (stretch) | Monthly ingest, retrain, drift report | PDF Week 6 |

Why each store fits its access pattern: Parquet on GCS for large scans by year and month; MongoDB for small documents looked up by key (route, airport) many times per second; DuckDB because EDA on Parquet needs no cluster.

## 4. Stages

### Stage 0 · Setup
Repo, `env.sh.example`, `.gitignore`, `setup.sh` (enable APIs, create bucket), `log_time.sh` for `timestamps.txt`, `cleanup.sh`. Free tier only: do not upgrade the billing account.

### Stage 1 · Ingest
Sample mode (2024, 30 busiest airports) runs in Cloud Shell in minutes and is the grader path. Full mode runs on a small VM in `tmux`, deleted when done. The FAA poller starts as early as possible on a free-tier e2-micro, writing one snapshot per minute to `stream/faa/`, so weeks of live data exist by the demo. Output: `verify_stage1.sh` report of files and bytes.

### Stage 2 · Transform (Spark)
Convert flight CSVs to Parquet partitioned by Year/Month, normalising columns that differ by era. Clean weather: keep routine hourly reports, drop readings with bad quality flags, convert units, one row per station-hour. Join each flight to origin and destination weather from the hour before scheduled departure.

### Stage 3 · EDA (DuckDB, charts in a notebook)
Shape: rows by year, carriers, airports. Distributions: arrival delay, late rate by hour, month, carrier, route. Missing values: by column and era, weather coverage by airport, station match distances. Outliers: extreme and negative delays, impossible durations, cancelled and diverted flights. Surprises. Every finding ends in a design decision ("X, so we did Y"). This stage carries 40 points and is not optional.

### Stage 4 · Features and model (Spark)
Window functions for rolling 30-day route and airport-hour late rates, computed only from earlier dates. Logistic regression in MLlib, trained on earlier years and tested on 2025. Report AUC, calibration (predicted vs actual late rate by bucket), and a naive baseline (route history alone). Save parameters to GCS for serving.

### Stage 5 · MongoDB and checks
Write `routes` and `airports` documents. Ask it twice: ORD's 2024 late rate (and one route-level number) computed in Spark SQL, in DuckDB, and in a MongoDB aggregation, and shown to agree. Check against the source: monthly on-time rates compared to BTS's published summaries.

### Stage 6 · Streaming (Spark Structured Streaming)
Read new FAA snapshots from `stream/faa/` as they land, parse the XML, turn changes into start and end events for ground stops and delay programs, and upsert `airport_status` in MongoDB. Run with an `availableNow` trigger so the cluster only exists while processing, plus one live run during the recording.

### Stage 7 · Serve and show
Cloud Run API: reads route, airport, and live status from MongoDB, loads the model from GCS, returns a probability plus the route's record. Streamlit dashboard of the findings. Claude with the MongoDB MCP server answering questions such as "Which ORD departures are riskiest in winter evenings?"

### Stage 8 · Measure and wrap up
Scaling decision: Cloud Run min-instances 0 vs 2 under a fixed load test, reporting p95, cold starts, and cost. Then README, architecture diagram PNG, decisions one-pager, `prompts.md`, deck, recording, and a full cleanup with timestamps.

## 5. Timeline

| Dates | Work | Milestone |
|---|---|---|
| Nov 8 – 11 | Stages 0–1 in sample mode; start full load on VM; start FAA poller | Data in GCS |
| Nov 11 – 16 | Stage 2 on sample; first EDA charts; check-in slides | **Nov 16 check-in slides; present Nov 17** |
| Nov 17 – 24 | Stage 2 at full scale; EDA complete; Stage 4 model | Model evaluated |
| Nov 24 – Dec 1 | Stages 5–6 (Thanksgiving is Nov 26, plan light) | Checks agree; stream running |
| Dec 1 – 6 | Stage 7; load test | Demo works end to end |
| Dec 7 – 11 | README, deck, recording, cleanup | **Submit Dec 11, 11:59 PM CT** |

## 6. Rubric coverage

| Requirement | Where it is met |
|---|---|
| Business problem, named audience (20) | Section 1; first slide |
| Data sources and challenges, big data (20) | Section 2; Stages 1–2 |
| EDA (40) | Stage 3 notebook and charts |
| Complex queries, checked, performance (40) | Stage 2 join, Stage 4 windows, Stage 5 checks; partition pruning and file sizes noted |
| Architecture and pipeline, three+ tools (40) | Section 3; diagram; runs from README |
| Findings and presentation, demo (40) | Stage 7 dashboard, API, MCP demo; recording |
| PDF: one scaling decision, measured | Stage 8 |
| PDF: timestamps, cleanup, prompts.md, decisions page | Stage 0 scripts; Stage 8 |
| No secrets anywhere | `env.sh` git-ignored; MCP connection string never shown on screen |

## 7. Risks

| Risk | Plan |
|---|---|
| BTS server slow or failing | Retries and GitHub mirror; resumable loader |
| Free-trial CPU cap limits cluster size | Small clusters; check quota before Stage 2 |
| Atlas M0 limit (512 MB) | Store only summaries in MongoDB; flights stay in Parquet |
| Station list format differs from expectation | Script prints header and match rate; adjust once |
| Which requirement document applies | Email Jan this week |
| Live-weather endpoint at serve time | Verify the NWS observation endpoint before Stage 7 |
