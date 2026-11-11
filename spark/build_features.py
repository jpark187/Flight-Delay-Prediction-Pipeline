"""Stage 4.1 - feature engineering: one row per operated flight, 2018-2025, with ONLY information
available before scheduled departure.

Inputs  {base}/curated/flights_wx  (Stage 2 join; 2016+ read so 2018 has a full year of history)
Output  {base}/curated/features/year=YYYY/month=M/
        {base}/curated/checks/features_summary/   rows, late rate and feature coverage by split

Features
  schedule   dep hour (local), month, day of week, carrier, distance, scheduled duration
  history    rolling late rates over the 30 (and 365) days BEFORE the flight's date for the route,
             origin airport, origin airport x departure hour, destination arrivals and carrier;
             route rate shrunk toward the origin's rate when the route has few flights
  rotation   legs the same aircraft flies earlier that day; scheduled turnaround minutes
  weather    origin and destination, last report before departure (from Stage 2): visibility,
             ceiling, wind, gusts, precipitation, temperature, decoded present weather
Never inputs (known only after departure): dep/arr delay, delay causes, cancellation, diversion.
Split: train 2018, 2019, 2021-2023 | covid 2020 (kept, flagged) | valid 2024 | test 2025
"""
from pyspark.sql import functions as F, Window
from common import parse_args, spark_session

args = parse_args(__doc__)
spark = spark_session("build_features")
base = args.base
first_year = 2024 if args.mode == "sample" else 2016    # history lookback
out_from = 2024 if args.mode == "sample" else 2018

WX = ["temp_c", "visibility_km", "ceiling_m", "wind_speed_ms", "wind_gust_ms", "precip_mm", "pres_wx"]
cols = (["flight_date", "year", "month", "day_of_week", "carrier", "flight_num", "tail_num", "origin",
         "dest", "crs_dep_hhmm", "crs_arr_hhmm", "crs_elapsed_min", "distance_mi", "arr_del15",
         "cancelled", "diverted", "crs_dep_local", "o_station", "d_station"]
        + [p + c for p in ("o_", "d_") for c in WX])
f = spark.read.parquet(f"{base}/curated/flights_wx").filter(F.col("year") >= first_year).select(*cols)

# ---------------------------------------------------------------- clean (EDA section 9)
f = (f.filter((F.col("cancelled") == 0) & F.col("arr_del15").isNotNull())        # operated flights
      .filter(F.col("crs_elapsed_min").between(1, 900) & (F.col("distance_mi") > 0)
              & (F.col("origin") != F.col("dest")))
      .dropDuplicates(["flight_date", "carrier", "flight_num", "origin", "crs_dep_hhmm"])
      .withColumn("day", F.datediff("flight_date", F.lit("1970-01-01")))
      .withColumn("dep_hour", F.hour("crs_dep_local")))

# ---------------------------------------------------------------- rolling history (strictly earlier days)
K = 20.0          # shrinkage strength: a route needs ~20 flights before its own rate dominates

def rolling(keys, name, days):
    """late rate for `keys` over the `days` days before each flight's date (current day excluded)."""
    daily = f.groupBy(*keys, "day").agg(F.count("*").alias("n"), F.sum("arr_del15").alias("late"))
    w = Window.partitionBy(*keys).orderBy("day").rangeBetween(-days, -1)
    return (daily.select(*keys, "day",
                         F.sum("n").over(w).alias(f"{name}_n"),
                         F.sum("late").over(w).alias(f"{name}_late")))

hist = [
    (["origin", "dest"], "route30", 30), (["origin", "dest"], "route365", 365),
    (["origin"], "orig30", 30), (["origin", "dep_hour"], "orighr30", 30),
    (["dest"], "dest30", 30), (["carrier"], "carr30", 30),
]
for keys, name, days in hist:
    f = f.join(rolling(keys, name, days), keys + ["day"], "left")

GLOBAL = 0.20     # fallback when there is no history at all (overall late rate, EDA section 1)
rate = lambda name: F.col(f"{name}_late") / F.col(f"{name}_n")
f = f.withColumn("orig30_rate", F.coalesce(rate("orig30"), F.lit(GLOBAL)))
for name in ["orighr30", "dest30", "carr30"]:
    f = f.withColumn(f"{name}_rate", F.coalesce(rate(name), F.lit(GLOBAL)))
for name in ["route30", "route365"]:      # shrink sparse routes toward the origin airport's rate
    n, late = F.coalesce(F.col(f"{name}_n"), F.lit(0)), F.coalesce(F.col(f"{name}_late"), F.lit(0))
    f = (f.withColumn(f"{name}_rate", (late + K * F.col("orig30_rate")) / (n + K))
          .withColumn(f"{name}_logn", F.log1p(n)))

# ---------------------------------------------------------------- aircraft rotation (from the schedule)
def local_ts(date_col, hhmm_col):
    h = F.col(hhmm_col)
    d = F.when(h == 2400, F.date_add(date_col, 1)).otherwise(F.col(date_col))
    return F.to_timestamp(F.concat_ws(" ", d.cast("string"),
                          F.lpad(F.when(h == 2400, 0).otherwise(h).cast("string"), 4, "0")), "yyyy-MM-dd HHmm")

tail_ok = F.length(F.regexp_replace("tail_num", "[^A-Za-z0-9]", "")) >= 3
arr_local = local_ts("flight_date", "crs_arr_hhmm")
arr_local = F.when(F.col("crs_arr_hhmm") < F.col("crs_dep_hhmm"), arr_local + F.expr("INTERVAL 1 DAY")).otherwise(arr_local)
f = f.withColumn("crs_arr_local_est", arr_local).withColumn("tail_ok", tail_ok.cast("int"))
wt = Window.partitionBy("tail_num", "flight_date").orderBy("crs_dep_local")
prev_dest, prev_arr = F.lag("dest").over(wt), F.lag("crs_arr_local_est").over(wt)
secs = lambda c: c.cast("timestamp").cast("long")
turn = (secs(F.col("crs_dep_local")) - secs(prev_arr)) / 60
f = (f.withColumn("legs_before", F.when(F.col("tail_ok") == 1, F.row_number().over(wt) - 1))
      .withColumn("turn_min", F.when((F.col("tail_ok") == 1) & (prev_dest == F.col("origin"))
                                     & turn.between(0, 1440), turn)))

# ---------------------------------------------------------------- weather (EDA sections 7-8)
def pres_wx_flags(p):
    """present weather, e.g. 'RA:61', '-SN:03', 'TS:95', '00'. Codes 00-03 and blank = nothing
    significant (the pre-2007 and post-2007 encodings mean the same thing)."""
    s = F.upper(F.coalesce(F.col(p), F.lit("")))
    word = F.regexp_extract(s, r"^([+-]?)([A-Z]*)", 2)
    sign = F.regexp_extract(s, r"^([+-]?)", 1)
    def has(*codes):
        cond = F.lit(False)
        for c in codes:
            cond = cond | word.contains(c)
        return cond
    return {
        "thunder": has("TS"), "snow": has("SN", "SG", "PL", "IC"), "freezing": has("FZ"),
        "rain": has("RA", "DZ"), "showers": has("SH"), "fog": has("FG"), "mist_haze": has("BR", "HZ", "FU"),
        "heavy": (sign == "+"), "light": (sign == "-"),
    }

for p in ("o_", "d_"):
    f = (f.withColumn(f"{p}wx_missing", F.col(f"{p}temp_c").isNull().cast("int"))
          .withColumn(f"{p}vis", F.coalesce(F.least(F.col(f"{p}visibility_km"), F.lit(16.0)), F.lit(16.0)))
          .withColumn(f"{p}ceil_low", (F.coalesce(F.col(f"{p}ceiling_m"), F.lit(30000.0)) < 300).cast("int"))
          .withColumn(f"{p}ceil_km", F.least(F.coalesce(F.col(f"{p}ceiling_m"), F.lit(30000.0)) / 1000, F.lit(5.0)))
          .withColumn(f"{p}wind", F.coalesce(F.col(f"{p}wind_speed_ms"), F.lit(0.0)))
          .withColumn(f"{p}gust", F.coalesce(F.col(f"{p}wind_gust_ms"), F.lit(0.0)))
          .withColumn(f"{p}precip_missing", F.col(f"{p}precip_mm").isNull().cast("int"))
          .withColumn(f"{p}precip", F.coalesce(F.col(f"{p}precip_mm"), F.lit(0.0)))
          .withColumn(f"{p}temp", F.coalesce(F.col(f"{p}temp_c"), F.lit(15.0)))
          .withColumn(f"{p}hot", (F.col(f"{p}temp") >= 30).cast("int"))
          .withColumn(f"{p}freezing_temp", (F.col(f"{p}temp") <= 0).cast("int")))
    for k, v in pres_wx_flags(f"{p}pres_wx").items():
        f = f.withColumn(f"{p}wx_{k}", v.cast("int"))

# ---------------------------------------------------------------- output
split = (F.when(F.col("year") == 2025, "test").when(F.col("year") == 2024, "valid")
          .when(F.col("year") == 2020, "covid").otherwise("train"))
FEATURES = (["dep_hour", "month", "day_of_week", "carrier", "distance_mi", "crs_elapsed_min",
             "legs_before", "turn_min", "tail_ok"]
            + [f"{n}_rate" for n in ["route30", "route365", "orig30", "orighr30", "dest30", "carr30"]]
            + ["route30_logn", "route365_logn"]
            + [f"{p}{c}" for p in ("o_", "d_") for c in
               ["wx_missing", "vis", "ceil_low", "ceil_km", "wind", "gust", "precip", "precip_missing",
                "temp", "hot", "freezing_temp", "wx_thunder", "wx_snow", "wx_freezing", "wx_rain",
                "wx_showers", "wx_fog", "wx_mist_haze", "wx_heavy", "wx_light"]])
ID = ["flight_date", "carrier_code", "flight_num", "origin", "dest", "crs_dep_local"]
out = (f.filter(F.col("year") >= out_from)
        .withColumn("split", split).withColumn("carrier_code", F.col("carrier"))
        .select(*ID, *FEATURES, F.col("arr_del15").alias("label"), "split", "year"))

(out.repartition("year", "month").write.mode("overwrite").partitionBy("year", "month")
    .parquet(f"{base}/curated/features"))

res = spark.read.parquet(f"{base}/curated/features")
summ = res.groupBy("split").agg(
    F.count("*").alias("rows"), F.avg("label").alias("late_rate"),
    F.avg(F.col("legs_before").isNotNull().cast("int")).alias("has_rotation"),
    F.avg(F.col("turn_min").isNotNull().cast("int")).alias("has_turnaround"),
    F.avg(1 - F.col("o_wx_missing")).alias("has_origin_wx"),
    F.avg((F.col("route30_logn") > 0).cast("int")).alias("has_route_history"),
    F.avg("o_wx_thunder").alias("origin_thunder"), F.avg("o_wx_snow").alias("origin_snow"))
summ.coalesce(1).write.mode("overwrite").option("header", True).csv(f"{base}/curated/checks/features_summary")
summ.orderBy("split").show(truncate=False)
print("feature columns:", len(FEATURES))
