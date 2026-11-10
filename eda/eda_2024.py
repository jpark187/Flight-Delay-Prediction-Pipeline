"""eda_2024.py - exploratory data analysis of the joined flights + weather table (2024 sample).

Profiles shape, the target (late by 15+ min), delay distribution, late rate by time and place,
missing values, outliers, delay causes and weather effects. Writes charts and a summary:
    results/eda/*.png
    results/eda/eda_summary.md

Run in Cloud Shell from the repo root (downloads ~200 MB once):
    pip install --user duckdb pandas matplotlib
    python3 eda/eda_2024.py
Uses DuckDB directly on the Parquet files: no cluster needed for one year.
"""
import argparse, os, subprocess, textwrap
import duckdb
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

ap = argparse.ArgumentParser()
ap.add_argument("--data", default=os.path.expanduser("~/data/flights_wx"))
ap.add_argument("--bucket", default=os.environ.get("BUCKET", ""))
ap.add_argument("--out", default="results/eda")
args = ap.parse_args()
os.makedirs(args.out, exist_ok=True)

if not os.path.isdir(args.data):
    if not args.bucket:
        raise SystemExit("no local data: run `source env.sh` first so BUCKET is set")
    print(f"downloading {args.bucket}/curated/flights_wx -> {args.data}")
    os.makedirs(os.path.dirname(args.data), exist_ok=True)
    subprocess.run(["gcloud", "storage", "cp", "-r", f"{args.bucket}/curated/flights_wx",
                    os.path.dirname(args.data)], check=True)

con = duckdb.connect()
con.execute(f"""CREATE VIEW f AS SELECT * FROM
    read_parquet('{args.data}/**/*.parquet', hive_partitioning = 1)""")
q = lambda sql: con.execute(sql).df()
one = lambda sql: con.execute(sql).fetchone()
md = ["# EDA: 2024 sample (flights joined to weather)\n"]
def section(title): md.append(f"\n## {title}\n")
def table(df):
    """markdown table without extra dependencies"""
    fmt = lambda v: (f"{v:.3f}" if isinstance(v, float) and abs(v) < 1000 else
                     f"{v:,.0f}" if isinstance(v, (int, float)) else str(v))
    md.append("| " + " | ".join(map(str, df.columns)) + " |")
    md.append("|" + "---|" * len(df.columns))
    for row in df.itertuples(index=False):
        md.append("| " + " | ".join(fmt(v) for v in row) + " |")
    md.append("")

plt.rcParams.update({"figure.dpi": 120, "axes.spines.top": False, "axes.spines.right": False,
                     "axes.grid": True, "grid.alpha": 0.3, "font.size": 10})
BLUE, RED, GREY = "#2b6cb0", "#c53030", "#718096"
def save(fig, name):
    fig.tight_layout(); fig.savefig(f"{args.out}/{name}"); plt.close(fig); print("wrote", name)

# ------------------------------------------------------------------ 1. shape
section("1. Shape")
n, ncol = one("SELECT count(*) FROM f")[0], len(q("DESCRIBE f"))
shape = q("""SELECT min(flight_date) first_day, max(flight_date) last_day,
    count(DISTINCT carrier) carriers, count(DISTINCT origin) origins,
    count(DISTINCT origin || '-' || dest) routes, count(DISTINCT tail_num) aircraft,
    avg(cancelled) cancel_rate, avg(diverted) divert_rate,
    avg(arr_del15) late_rate_operated FROM f""")
md.append(f"{n:,} flights, {ncol} columns.\n"); table(shape)

# ------------------------------------------------------------------ 2. target + delay distribution
section("2. Arrival delay distribution")
dist = q("""SELECT quantile_cont(arr_delay_min, [0.01,0.05,0.25,0.5,0.75,0.95,0.99]) qs,
    avg(arr_delay_min) mean, max(arr_delay_min) max, min(arr_delay_min) min,
    avg((arr_delay_min < 0)::int) early_share FROM f WHERE arr_delay_min IS NOT NULL""")
qs = dist.qs[0]
md.append("Quantiles (1, 5, 25, 50, 75, 95, 99%): " + ", ".join(f"{v:.0f}" for v in qs) + " minutes.  ")
md.append(f"Mean {dist['mean'][0]:.1f}, min {dist['min'][0]:.0f}, max {dist['max'][0]:.0f}; "
          f"{dist.early_share[0]:.1%} of operated flights arrive early.\n")
h = q("""SELECT floor(least(greatest(arr_delay_min, -60), 300) / 5) * 5 AS bin, count(*) n
         FROM f WHERE arr_delay_min IS NOT NULL GROUP BY 1 ORDER BY 1""")
fig, ax = plt.subplots(figsize=(8, 4))
ax.bar(h.bin, h.n, width=5, color=[RED if b >= 15 else BLUE for b in h.bin], align="edge")
ax.axvline(15, color="k", lw=1, ls="--"); ax.text(17, h.n.max() * .9, "late = 15+ min", fontsize=9)
ax.set_yscale("log"); ax.set_xlabel("arrival delay (minutes, clipped to -60 … 300)")
ax.set_ylabel("flights (log scale)"); ax.set_title("Arrival delay distribution")
save(fig, "01_delay_distribution.png")

# ------------------------------------------------------------------ 3. time patterns
section("3. Late rate by scheduled departure hour, month, day of week")
hr = q("""SELECT crs_dep_hhmm // 100 AS hour, count(*) n, avg(arr_del15) late
          FROM f WHERE arr_del15 IS NOT NULL AND crs_dep_hhmm BETWEEN 0 AND 2400
          GROUP BY 1 ORDER BY 1""")
mo = q("SELECT month, count(*) n, avg(arr_del15) late FROM f WHERE arr_del15 IS NOT NULL GROUP BY 1 ORDER BY 1")
dw = q("SELECT day_of_week, count(*) n, avg(arr_del15) late FROM f WHERE arr_del15 IS NOT NULL GROUP BY 1 ORDER BY 1")
table(hr.assign(late=hr.late.round(3)))
fig, ax = plt.subplots(figsize=(8, 4))
ax.plot(hr.hour, hr.late * 100, marker="o", color=RED)
ax2 = ax.twinx(); ax2.bar(hr.hour, hr.n, color=GREY, alpha=.25); ax2.set_ylabel("flights", color=GREY)
ax2.grid(False); ax.set_zorder(ax2.get_zorder() + 1); ax.patch.set_visible(False)
ax.set_xlabel("scheduled departure hour (local time)"); ax.set_ylabel("late rate (%)")
ax.set_title("Late rate by scheduled departure hour"); ax.set_xticks(range(0, 25, 2))
save(fig, "02_late_rate_by_hour.png")
fig, (a1, a2) = plt.subplots(1, 2, figsize=(9, 3.6), sharey=True)
a1.bar(mo.month, mo.late * 100, color=BLUE); a1.set_xticks(range(1, 13)); a1.set_title("by month")
a1.set_ylabel("late rate (%)")
a2.bar(dw.day_of_week, dw.late * 100, color=BLUE); a2.set_xticks(range(1, 8))
a2.set_xticklabels(["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]); a2.set_title("by day of week")
save(fig, "03_late_rate_month_dow.png")

# ------------------------------------------------------------------ 4. carriers and airports
section("4. Carriers and airports")
ca = q("""SELECT carrier, count(*) n, avg(arr_del15) late, avg(cancelled) cancelled
          FROM f GROUP BY 1 ORDER BY n DESC""")
table(ca)
ap20 = q("""SELECT origin, count(*) n, avg(arr_del15) late FROM f
            GROUP BY 1 ORDER BY n DESC LIMIT 20""")
table(ap20)
fig, (a1, a2) = plt.subplots(1, 2, figsize=(11, 4.5))
c = ca.sort_values("late")
a1.barh(c.carrier, c.late * 100, color=BLUE); a1.set_xlabel("late rate (%)"); a1.set_title("Carriers")
a = ap20.sort_values("late")
a2.barh(a.origin, a.late * 100, color=BLUE); a2.set_xlabel("late rate (%)")
a2.set_title("20 busiest origin airports")
save(fig, "04_carriers_airports.png")
rt = one("""SELECT quantile_cont(late, 0.1), quantile_cont(late, 0.9), count(*) FROM
            (SELECT avg(arr_del15) late FROM f WHERE arr_del15 IS NOT NULL
             GROUP BY origin, dest HAVING count(*) >= 500)""")
md.append(f"Routes with 500+ flights: {rt[2]:,}; route late rate ranges from {rt[0]:.1%} (10th pct) "
          f"to {rt[1]:.1%} (90th pct).\n")

# ------------------------------------------------------------------ 5. missing values
section("5. Missing values")
cols = [r[0] for r in con.execute("DESCRIBE f").fetchall()]
nulls = q("SELECT " + ", ".join(f"avg(({c} IS NULL)::int) AS \"{c}\"" for c in cols) + " FROM f")
miss = nulls.T.reset_index(); miss.columns = ["column", "null_share"]
miss = miss.sort_values("null_share", ascending=False)
table(miss[miss.null_share > 0])
m = miss[miss.null_share > 0].head(30).sort_values("null_share")
fig, ax = plt.subplots(figsize=(8, max(3, .25 * len(m))))
ax.barh(m.column, m.null_share * 100, color=GREY); ax.set_xlabel("% missing")
ax.set_title("Missing values by column (columns with any)")
save(fig, "05_missing_values.png")
why = one("""SELECT avg((arr_del15 IS NULL)::int),
             avg(CASE WHEN arr_del15 IS NULL THEN (cancelled = 1 OR diverted = 1)::int END),
             avg(CASE WHEN carrier_delay_min IS NULL THEN (arr_del15 = 0 OR arr_del15 IS NULL)::int END)
             FROM f""")
md.append(f"Target missing for {why[0]:.2%} of flights; {why[1]:.1%} of those are cancelled or diverted.  ")
md.append(f"Delay-cause minutes are missing for on-time flights by design ({why[2]:.1%} of their nulls).\n")

# ------------------------------------------------------------------ 6. outliers and data errors
section("6. Outliers and data errors")
out = q("""SELECT
    sum((arr_delay_min > 720)::int)                     AS late_over_12h,
    sum((arr_delay_min < -90)::int)                     AS early_over_90min,
    sum((crs_elapsed_min <= 0 OR crs_elapsed_min > 900)::int) AS bad_sched_duration,
    sum((distance_mi <= 0)::int)                        AS zero_distance,
    sum((crs_dep_hhmm % 100 >= 60 OR crs_dep_hhmm > 2400)::int) AS bad_dep_time,
    sum((origin = dest)::int)                           AS same_origin_dest
    FROM f""")
table(out)
dup = one("""SELECT count(*), sum(n) FROM (SELECT count(*) n FROM f
             GROUP BY flight_date, carrier, flight_num, origin, crs_dep_hhmm HAVING count(*) > 1)""")
md.append(f"Duplicate keys (date, carrier, flight number, origin, scheduled time): {dup[0] or 0:,} groups.\n")

# ------------------------------------------------------------------ 7. delay causes
section("7. What causes the delay minutes (late flights)")
cause = q("""SELECT sum(carrier_delay_min) carrier, sum(weather_delay_min) weather,
    sum(nas_delay_min) nas, sum(security_delay_min) security,
    sum(late_aircraft_delay_min) late_aircraft FROM f WHERE arr_del15 = 1""").T.reset_index()
cause.columns = ["cause", "minutes"]; cause["share"] = cause.minutes / cause.minutes.sum()
table(cause)
fig, ax = plt.subplots(figsize=(7, 3.4))
cc = cause.sort_values("share")
ax.barh(cc.cause, cc.share * 100, color=[RED if c == "late_aircraft" else BLUE for c in cc.cause])
ax.set_xlabel("% of delay minutes on late flights"); ax.set_title("Share of delay minutes by cause")
save(fig, "06_delay_causes.png")

# ------------------------------------------------------------------ 8. weather
section("8. Weather at the origin before departure")
cov = one("SELECT avg((o_temp_c IS NOT NULL)::int) FROM f")[0]
md.append(f"Flights with origin weather: {cov:.1%} (sample has weather for 30 airports).\n")
W = {
  "visibility_km": ("visibility (km)", [0, 1, 3, 5, 10, 1e9], ["<1", "1-3", "3-5", "5-10", "10+"]),
  "wind_speed_ms": ("wind (m/s)",      [0, 3, 6, 9, 12, 1e9], ["0-3", "3-6", "6-9", "9-12", "12+"]),
  "temp_c":        ("temperature (°C)", [-1e9, -10, 0, 10, 20, 30, 1e9],
                    ["<-10", "-10-0", "0-10", "10-20", "20-30", "30+"]),
  "precip_mm":     ("precipitation, past hour (mm)", [0, 1e-9, 1, 1e9], ["none", "<1", "1+"]),
}
fig, axes = plt.subplots(1, len(W), figsize=(13, 3.6), sharey=True)
for ax, (col, (label, edges, names)) in zip(axes, W.items()):
    case = "CASE " + " ".join(f"WHEN o_{col} >= {lo} AND o_{col} < {hi} THEN {k}"
                              for k, (lo, hi) in enumerate(zip(edges[:-1], edges[1:]))) + " END"
    b = q(f"""SELECT {case} AS k, count(*) n, avg(arr_del15) late FROM f
              WHERE arr_del15 IS NOT NULL AND o_{col} IS NOT NULL GROUP BY 1 ORDER BY 1""").dropna()
    b["bin"] = [names[int(k)] for k in b.k]
    md.append(f"**{label}**"); table(b[["bin", "n", "late"]])
    ax.bar(b.bin, b.late * 100, color=BLUE); ax.set_title(label, fontsize=9)
    for x, (v, nn) in enumerate(zip(b.late, b.n)):
        ax.text(x, v * 100, f"{nn/1000:.0f}k", ha="center", va="bottom", fontsize=7, color=GREY)
axes[0].set_ylabel("late rate (%)")
fig.suptitle("Late rate by origin weather in the hour before departure (labels: flights)")
save(fig, "07_weather_effects.png")
pw = q("""SELECT o_pres_wx, count(*) n, avg(arr_del15) late FROM f
          WHERE o_pres_wx IS NOT NULL GROUP BY 1 ORDER BY n DESC LIMIT 15""")
md.append("**Present-weather codes, most common (format to be decoded):**"); table(pw)

with open(f"{args.out}/eda_summary.md", "w") as fh:
    fh.write("\n".join(md))
print(f"wrote {args.out}/eda_summary.md")
