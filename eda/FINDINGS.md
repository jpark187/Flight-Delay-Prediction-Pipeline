# EDA findings and the design decisions they drive

Sources: `results/eda_full/eda_full_summary.md` (all 230,889,993 flights, October 1987 to
December 2025, aggregated by Spark with `spark/eda_aggregates.py`) and
`results/eda/eda_summary.md` (2024 in full detail, queried with DuckDB). Each finding ends in the
decision it drives.

## 1. The target: late arrival on operated flights, about one flight in five

Across 38 years, 19.7% of operated flights arrive 15+ minutes late. The yearly rate stays between
16.6% and 24.8%, with one exception: 2020, when COVID cut it to 9.8% while cancellations rose to
6.0%. Cancellations also spike in 2001 (3.9%, after 9/11). The target is missing only for cancelled
and diverted flights (1.1–3.0% a year).

**Decision:** predict late arrival for flights that operate; report cancellation separately. With
a 20% positive rate, the model is judged on AUC and calibration rather than accuracy, where
"always on time" already scores 80%.

## 2. Schedules got padded while the bad days got worse

The median arrival moved from 4 minutes late in 1987 to 6 minutes early from 2016 on, and the
share of early arrivals nearly doubled (32.6% to 62.0%). Over the same years the 99th-percentile
delay more than doubled, from about 100 minutes to 224, and flights more than 12 hours late went
from about 50 a year to about 7,500 a year (2023–2025). The typical flight looks better on paper;
the tail is far longer.

**Decision:** predict the 15-minute threshold rather than delay minutes, so the growing tail cannot
dominate the fit. Route and airport summaries store late rates and medians, not mean minutes.

## 3. The data has structural breaks, so the model trains on one consistent period

The number of reporting carriers jumps from 10 to 18 in 2003 and from 12 to 18 in 2018, and the
airports covered jump with it (218 to 282, 320 to 358). These are changes in who reports to BTS,
as regional airlines were added, not changes in US flying. 34 carrier codes appear over the period
as airlines merge and disappear. Weather relationships also shift over time (section 7).

**Decision:** train on 2018–2023 (one reporting regime), validate on 2024, test on 2025. 2020 is
excluded from training, or flagged and tested both ways, because COVID changed both traffic and
delay behavior. The older years support the EDA, long-run trends and the cross-source check.

## 4. Time of day and season are stable, strong patterns

In every era, the late rate rises from 7–9% for 5 AM departures to a peak of 26–29% at 7–8 PM, as
delays accumulate through the day. December (24.0%), June (23.1%) and July (22.4%) are the worst
months; September (15.1%) is the best.

**Decision:** scheduled local departure hour and month are core features. Early-morning hours with
few flights are grouped.

## 5. Delays propagate through the day; carrier-caused delay rose after COVID

Since BTS began recording causes in mid-2003, late-arriving aircraft grew from 30.9% of delay
minutes to about 40% (2010–2019, and again from 2023), and airspace-system (NAS) delay fell from 36.5% to 17–22%.
Carrier-caused delay jumped to about 40–42% in 2020–2022, the post-COVID staffing years, and is easing
(32.5% in 2025). BTS labels only 4–7% as weather in every year.

**Decision:** test an aircraft-rotation feature: how many legs the same aircraft is scheduled to
fly earlier that day (known in advance). Tail numbers are recorded from 1995 on (missing for
every flight in 1987–1994), which does not affect the 2018+ training window. Carrier late rates
come from recent windows. Delay-cause columns are recorded after the flight and are never inputs.

## 6. Carriers, airports and routes differ, and airports change over time

2015–2025 carrier late rates range from 14.0% (Hawaiian), 14.1% (Endeavor) and 14.7% (Delta) to
26.0% (JetBlue) and 26.3% (Frontier). Airports shift: Atlanta went from 25.7% late in the 2000s to
17.1% in the 2010s; Orlando rose from 17.6% (1987–1999) to 24.1% (2020–2025). Among 4,634 routes
with 2,000+ flights in 2015–2025, late rates run from 13.0% (10th percentile) to 24.2% (90th).

**Decision:** route and airport rates are rolling windows computed only from earlier dates, not
all-time averages, and stored in MongoDB for the API. Sparse routes are shrunk toward the origin
airport's rate.

## 7. Weather matters far more than BTS labels suggest, but some effects have weakened

| Origin, hour before departure | 1987–1999 | 2020–2025 |
|---|---|---|
| Visibility 10+ km / under 1 km | 18.1% / 45.7% | 18.6% / 29.7% |
| Cloud ceiling 3 km+ / under 150 m | 16.8% / 37.9% | 18.0% / 23.5% |
| Wind 0–3 m/s / 12+ m/s | 16.9% / 34.5% | 16.4% / 33.1% |
| No precipitation / 1+ mm past hour | 18.6% / 40.5% | 18.4% / 36.1% |
| Temperature 30 °C+ | 19.7% | 26.9% |

Thunderstorms (TS, TS:95) bring a 55–56% late rate across all years, and snow 49–51% (SN:71,
-SN:03); in 2024 alone, moderate snow (SN:73) reached 90%. Fog and low cloud mattered much less in
the 2020s than in the 1990s, plausibly because of better instrument-landing capability, while
wind and precipitation effects held steady and hot weather became more disruptive.

**Decision:** keep visibility, ceiling, wind, precipitation and temperature as features, trained
on the recent era where the relationships hold today. Decode the present-weather field into type
(thunder, snow, rain, showers, drizzle, fog, mist, haze) and intensity (prefix `-` light, `+`
heavy), confirmed against NOAA's code table.

## 8. Missing values: most are explainable, and two fields changed encoding over time

| Column(s) | Pattern | Treatment |
|---|---|---|
| Weather fields (temperature, visibility, ceiling…) | 12% missing in 1987, under 1% from 1999, 0.1% recently | Missing indicator; negligible in the training window |
| Precipitation | 99% missing in 1987, 41–46% in 1991–2003, ~8% in 2007–2011, 1–2% from 2015 | Usable from 2015; training window is fine |
| Present weather | 3–13% missing before 2007 (code `00` = nothing significant), 86–89% after (left blank) | Treat blank and codes `00`–`02` as "none" so both eras mean the same thing |
| Wind gust | 83–94% missing every year | "No gust" plus an indicator |
| Wind direction | ~15% missing from 1999 (calm or variable) | Dropped; speed carries the signal |
| Snow depth | 93–99.8% missing | Dropped |
| Tail number | 100% missing 1987–1994 | Rotation feature only from 1995; training is 2018+ |
| Delay causes, cancellation code | Absent before 2003; otherwise only for late or cancelled flights | Never features |

## 9. Data errors are rare, mostly in the earliest years and 2003

Across 230.9M flights: 4,392 duplicate flight keys (8,799 rows, under 0.004%), concentrated in
1987–1989 and 2003 (3,113 groups, when the new reporting carriers joined); 394 implausible
scheduled durations; 7 zero-distance and 7 same-origin-and-destination flights, all in 1987–1989.

**Decision:** remove exact duplicates (keep one row per key) and drop the rows with impossible
values. BTS is clean overall, as the instructor noted; the engineering in this project is in
stitching two sources, matching weather stations, converting time zones and resolving renamed
airport codes.

## What surprised us

The typical flight now arrives early while 12-hour delays are a hundred times more common than in
the late 1980s. Fog and low cloud cost far less than they used to, yet hot weather costs more.
Weather is labelled as the cause of only about 5% of delay minutes, yet thunderstorms before
departure make a late arrival more likely than not. The two sources agree exactly in all 24
overlap months, and one airport (PBI) had been renamed in the reference data, which a coverage
check caught.

## Columns excluded from the model (known only after departure)

`dep_delay_min`, `arr_delay_min`, all five delay-cause columns, `cancellation_code`, `diverted`,
and `cancelled` (which defines the population rather than predicting it).
