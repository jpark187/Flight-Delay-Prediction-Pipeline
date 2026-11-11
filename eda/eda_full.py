"""eda_full.py - charts and summary for the full 1987-2025 history, from the aggregate tables
written by spark/eda_aggregates.py (run scripts/run_eda_workflow.sh first).

    source env.sh && python3 eda/eda_full.py
Output: results/eda_full/*.png and results/eda_full/eda_full_summary.md
"""
import glob, os, subprocess
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

BUCKET = os.environ.get("BUCKET", "")
LOCAL = os.path.expanduser("~/data/eda")
OUT = "results/eda_full"
os.makedirs(OUT, exist_ok=True)
if BUCKET:
    os.makedirs(LOCAL, exist_ok=True)
    subprocess.run(["gcloud", "storage", "cp", "-r", f"{BUCKET}/curated/eda/*", LOCAL + "/"], check=True)

def load(name):
    files = glob.glob(f"{LOCAL}/{name}/*.csv")
    if not files:
        raise SystemExit(f"missing {name}: has the eda-full workflow finished?")
    return pd.read_csv(files[0])

plt.rcParams.update({"figure.dpi": 120, "axes.spines.top": False, "axes.spines.right": False,
                     "axes.grid": True, "grid.alpha": 0.3, "font.size": 10})
BLUE, RED, GREY = "#2b6cb0", "#c53030", "#718096"
ERA_COLORS = {"1987-1999": "#a0aec0", "2000-2009": "#63b3ed", "2010-2019": "#2b6cb0", "2020-2025": "#c53030"}
md = ["# EDA: full history, Oct 1987 - Dec 2025\n"]
def save(fig, name):
    fig.tight_layout(); fig.savefig(f"{OUT}/{name}"); plt.close(fig); print("wrote", name)
PLAIN = {"year", "bin", "hour", "month", "day_of_week", "peak_hour", "years"}
def table(df):
    def fmt(col, v):
        if str(col) in PLAIN and isinstance(v, (int, float)) and v == v:
            return str(int(v))
        if isinstance(v, float) and abs(v) < 1000:
            return f"{v:.3f}"
        return f"{v:,.0f}" if isinstance(v, (int, float)) else str(v)
    md.append("| " + " | ".join(map(str, df.columns)) + " |")
    md.append("|" + "---|" * len(df.columns))
    for row in df.itertuples(index=False):
        md.append("| " + " | ".join(fmt(c, v) for c, v in zip(df.columns, row)) + " |")
    md.append("")

# ---------------------------------------------------------------- 1. volume and outcome by year
y = load("yearly").sort_values("year")
md.append(f"## 1. Shape\n\n{y.flights.sum():,} flights, {y.year.min()}-{y.year.max()} "
          f"(1987 starts in October). Late rate over all operated flights: "
          f"{(y.late_rate * y.flights * (1 - y.cancel_rate - y.divert_rate)).sum() / (y.flights * (1 - y.cancel_rate - y.divert_rate)).sum():.1%}.\n")
table(y[["year", "flights", "late_rate", "cancel_rate", "p50_delay", "p90_delay", "p99_delay",
         "early_share", "carriers", "airports"]])
fig, ax = plt.subplots(figsize=(10, 4.2))
ax.bar(y.year, y.flights / 1e6, color=GREY, alpha=.35, label="flights (millions)")
ax.set_ylabel("flights (millions)", color=GREY)
ax2 = ax.twinx(); ax2.grid(False)
ax2.plot(y.year, y.late_rate * 100, color=RED, marker="o", ms=3, label="late rate")
ax2.plot(y.year, y.cancel_rate * 100, color=BLUE, marker="o", ms=3, label="cancel rate")
ax2.set_ylabel("% of flights")
for yr, txt in [(2001, "9/11"), (2008, "recession"), (2020, "COVID")]:
    if yr in set(y.year):
        ax2.annotate(txt, (yr, y.set_index("year").late_rate[yr] * 100), xytext=(0, 14),
                     textcoords="offset points", ha="center", fontsize=8, color=GREY)
ax2.legend(loc="upper left", fontsize=8)
ax.set_title("Flights, late rate and cancellation rate by year")
save(fig, "01_by_year.png")

# ---------------------------------------------------------------- 2. hour of day by era
eh = load("era_hour")
fig, ax = plt.subplots(figsize=(8, 4))
for era, g in eh[eh.hour.between(5, 23)].groupby("era"):
    ax.plot(g.hour, g.late * 100, marker="o", ms=3, label=era, color=ERA_COLORS.get(era))
ax.set_xlabel("scheduled departure hour (local)"); ax.set_ylabel("late rate (%)")
ax.set_title("Late rate by departure hour, by era (5 AM - 11 PM)"); ax.legend(fontsize=8)
save(fig, "02_hour_by_era.png")
pk = eh[eh.hour.between(5, 23)].groupby("era").apply(
    lambda g: pd.Series({"5am": g[g.hour == 5].late.mean(), "peak": g.late.max(),
                         "peak_hour": int(g.loc[g.late.idxmax(), "hour"])})).reset_index()
md.append("## 2. Departure hour, by era\n"); table(pk)

# ---------------------------------------------------------------- 3. year x month heatmap
ym = load("year_month").pivot(index="month", columns="year", values="late")
fig, ax = plt.subplots(figsize=(12, 3.8))
im = ax.imshow(ym.values * 100, aspect="auto", cmap="Reds")
ax.set_yticks(range(12)); ax.set_yticklabels(["J", "F", "M", "A", "M", "J", "J", "A", "S", "O", "N", "D"])
ax.set_xticks(range(0, len(ym.columns), 2)); ax.set_xticklabels(ym.columns[::2], rotation=90, fontsize=8)
fig.colorbar(im, ax=ax, label="late rate (%)"); ax.set_title("Late rate by month and year"); ax.grid(False)
save(fig, "03_month_year_heatmap.png")
mavg = load("year_month").groupby("month").apply(lambda g: (g.late * g.n).sum() / g.n.sum())
md.append("## 3. Seasonality (all years)\n")
md.append("Late rate by month: " + ", ".join(f"{m}: {v:.1%}" for m, v in mavg.items()) + "\n")

# ---------------------------------------------------------------- 4. carriers, recent decade
cy = load("carrier_year")
rec = cy[cy.year >= 2015].groupby("carrier").apply(
    lambda g: pd.Series({"n": g.n.sum(), "late": (g.late * g.n).sum() / g.n.sum(),
                         "years": g.year.nunique()})).reset_index().sort_values("n", ascending=False)
md.append("## 4. Carriers, 2015-2025\n"); table(rec)
r = rec[rec.n > 200_000].sort_values("late")
fig, ax = plt.subplots(figsize=(6, 4.5))
ax.barh(r.carrier, r.late * 100, color=BLUE); ax.set_xlabel("late rate (%)")
ax.set_title("Carrier late rate, 2015-2025 (200k+ flights)")
save(fig, "04_carriers_recent.png")
md.append(f"Carriers that appear at some point: {cy.carrier.nunique()}; "
          f"carriers in 1988: {cy[cy.year == 1988].carrier.nunique()}, in 2025: {cy[cy.year == 2025].carrier.nunique()}.\n")

ae = load("airport_era")
big = ae[ae.era == "2020-2025"].sort_values("n", ascending=False).head(15).origin
piv = ae[ae.origin.isin(big)].pivot(index="origin", columns="era", values="late").reindex(big)
md.append("**Late rate by era, 15 busiest airports of 2020-2025:**"); table(piv.reset_index())
rr = load("route_recent"); rr = rr[rr.n >= 2000]
md.append(f"Routes with 2,000+ flights in 2015-2025: {len(rr):,}; late rate 10th-90th percentile "
          f"{rr.late.quantile(.1):.1%} - {rr.late.quantile(.9):.1%}.\n")

# ---------------------------------------------------------------- 5. delay causes by year
cz = load("causes_year")
cz = cz[cz[["carrier", "weather", "nas", "security", "late_aircraft"]].sum(axis=1) > 0]
shares = cz.set_index("year")[["late_aircraft", "carrier", "nas", "weather", "security"]]
shares = shares.div(shares.sum(axis=1), axis=0)
fig, ax = plt.subplots(figsize=(10, 4))
ax.stackplot(shares.index, *(shares[c] * 100 for c in shares.columns), labels=shares.columns,
             colors=[RED, BLUE, "#63b3ed", "#68d391", GREY], alpha=.85)
ax.set_ylabel("% of delay minutes (late flights)"); ax.set_xlim(shares.index.min(), shares.index.max())
ax.legend(loc="upper left", fontsize=8, ncol=5); ax.set_title("What BTS records as the cause of delay minutes")
save(fig, "05_causes_by_year.png")
md.append("## 5. Delay causes (recorded from mid-2003)\n"); table(shares.reset_index().round(3))

# ---------------------------------------------------------------- 6. missing values by year
nu = load("nulls_year").set_index("year")
keep = [c for c in nu.columns if nu[c].max() > 0.001]
fig, ax = plt.subplots(figsize=(12, max(4, .22 * len(keep))))
im = ax.imshow(nu[keep].T.values * 100, aspect="auto", cmap="Greys", vmin=0, vmax=100)
ax.set_yticks(range(len(keep))); ax.set_yticklabels(keep, fontsize=7)
ax.set_xticks(range(0, len(nu.index), 2)); ax.set_xticklabels(nu.index[::2], rotation=90, fontsize=8)
fig.colorbar(im, ax=ax, label="% missing"); ax.grid(False)
ax.set_title("Missing values by column and year (what each era recorded)")
save(fig, "06_missing_by_year.png")
md.append("## 6. Missing values by year (columns with any)\n")
table(nu[keep].T.iloc[:, ::4].reset_index().rename(columns={"index": "column"}).round(3))

# ---------------------------------------------------------------- 7. outliers and duplicates
ol = load("outliers_year").merge(load("duplicates_year"), on="year", how="left").fillna(0)
md.append("## 7. Outliers and data errors by year\n"); table(ol)
md.append(f"Totals: " + ", ".join(f"{c} {int(ol[c].sum()):,}" for c in ol.columns if c != "year") + ".\n")

# ---------------------------------------------------------------- 8. weather effect, by era
wb = load("weather_bins_era")
LABELS = {"visibility_km": ["<1", "1-3", "3-5", "5-10", "10+"], "wind_speed_ms": ["0-3", "3-6", "6-9", "9-12", "12+"],
          "temp_c": ["<-10", "-10-0", "0-10", "10-20", "20-30", "30+"], "precip_mm": ["none", "<1", "1+"],
          "ceiling_m": ["<150", "150-300", "300-1k", "1k-3k", "3k+"]}
UNITS = {"visibility_km": "visibility (km)", "wind_speed_ms": "wind (m/s)", "temp_c": "temperature (°C)",
         "precip_mm": "precip past hour (mm)", "ceiling_m": "cloud ceiling (m)"}
fig, axes = plt.subplots(1, len(LABELS), figsize=(15, 3.8), sharey=True)
md.append("## 8. Weather at origin before departure, by era\n")
for ax, (var, labels) in zip(axes, LABELS.items()):
    g = wb[wb.variable == var]
    for era, e in g.groupby("era"):
        e = e.dropna(subset=["bin"]).sort_values("bin")
        ax.plot(e.bin, e.late * 100, marker="o", ms=3, label=era, color=ERA_COLORS.get(era))
    ax.set_xticks(range(len(labels))); ax.set_xticklabels(labels, fontsize=7, rotation=30)
    ax.set_title(UNITS[var], fontsize=9)
    t = g.dropna(subset=["bin"]).pivot(index="bin", columns="era", values="late")
    t.index = [labels[int(i)] for i in t.index]
    md.append(f"**{UNITS[var]}**"); table(t.reset_index().rename(columns={"index": "bin"}).round(3))
axes[0].set_ylabel("late rate (%)"); axes[-1].legend(fontsize=7)
fig.suptitle("Late rate by origin weather before departure, by era")
save(fig, "07_weather_by_era.png")
pw = load("pres_wx_top")
md.append("**Present-weather codes (top 25)**"); table(pw.head(25))

# ---------------------------------------------------------------- 9. delay distribution by era
dh = load("delay_hist_era")
fig, ax = plt.subplots(figsize=(8, 4))
for era, g in dh.groupby("era"):
    g = g.sort_values("bin")
    ax.plot(g.bin, g.n / g.n.sum() * 100, label=era, color=ERA_COLORS.get(era))
ax.axvline(15, color="k", lw=1, ls="--"); ax.set_yscale("log")
ax.set_xlabel("arrival delay (minutes, clipped to -60 … 300)"); ax.set_ylabel("% of flights (log)")
ax.set_title("Arrival delay distribution by era"); ax.legend(fontsize=8)
save(fig, "08_delay_dist_by_era.png")

with open(f"{OUT}/eda_full_summary.md", "w") as fh:
    fh.write("\n".join(md))
print(f"wrote {OUT}/eda_full_summary.md")
