"""EDA over the full history (230M flights): Spark computes small aggregate tables, which
eda/eda_full.py turns into charts locally. The joined table is too big for one machine
(and for Cloud Shell's disk), so the heavy lifting stays on the cluster.

Output: {base}/curated/eda/<table>/part-*.csv   (one small CSV per table)
"""
from pyspark.sql import functions as F
from common import parse_args, spark_session

args = parse_args(__doc__)
spark = spark_session("eda_aggregates")
base = args.base
f = spark.read.parquet(f"{base}/curated/flights_wx")

era = (F.when(F.col("year") < 2000, "1987-1999").when(F.col("year") < 2010, "2000-2009")
        .when(F.col("year") < 2020, "2010-2019").otherwise("2020-2025"))
f = f.withColumn("era", era).withColumn("hour", (F.col("crs_dep_hhmm") / 100).cast("int"))
op = f.filter(F.col("arr_del15").isNotNull())          # operated flights with a known outcome

def save(df, name):
    df.coalesce(1).write.mode("overwrite").option("header", True).csv(f"{base}/curated/eda/{name}")
    print("wrote", name)

late, n = F.avg("arr_del15").alias("late"), F.count("*").alias("n")

# 1. per year: volume, outcome, delay quantiles, breadth of the network
save(f.groupBy("year").agg(
        F.count("*").alias("flights"), F.avg("cancelled").alias("cancel_rate"),
        F.avg("diverted").alias("divert_rate"), F.avg("arr_del15").alias("late_rate"),
        F.avg("arr_delay_min").alias("mean_arr_delay"),
        F.expr("percentile_approx(arr_delay_min, array(0.5, 0.9, 0.99), 2000)").alias("q"),
        F.avg((F.col("arr_delay_min") < 0).cast("int")).alias("early_share"),
        F.countDistinct("carrier").alias("carriers"), F.countDistinct("origin").alias("airports"),
        F.approx_count_distinct(F.concat_ws("-", "origin", "dest")).alias("routes"))
      .select("*", F.col("q")[0].alias("p50_delay"), F.col("q")[1].alias("p90_delay"),
              F.col("q")[2].alias("p99_delay")).drop("q").orderBy("year"), "yearly")

# 2-4. time patterns
save(op.filter(F.col("hour").between(0, 24)).groupBy("era", "hour").agg(n, late).orderBy("era", "hour"), "era_hour")
save(f.groupBy("year", "month").agg(n, F.avg("arr_del15").alias("late"),
                                    F.avg("cancelled").alias("cancelled")).orderBy("year", "month"), "year_month")
save(op.groupBy("era", "day_of_week").agg(n, late).orderBy("era", "day_of_week"), "era_dow")

# 5-7. carriers, airports, routes
save(f.groupBy("carrier", "year").agg(n, F.avg("arr_del15").alias("late"),
                                      F.avg("cancelled").alias("cancelled")).orderBy("carrier", "year"), "carrier_year")
save(op.groupBy("origin", "era").agg(n, late).orderBy("origin", "era"), "airport_era")
save(op.filter(F.col("year") >= 2015).groupBy("origin", "dest").agg(n, late), "route_recent")

# 8. delay causes (BTS reports them from mid-2003)
save(f.filter(F.col("arr_del15") == 1).groupBy("year").agg(
        *[F.sum(c).alias(c.replace("_delay_min", "")) for c in
          ["carrier_delay_min", "weather_delay_min", "nas_delay_min", "security_delay_min",
           "late_aircraft_delay_min"]]).orderBy("year"), "causes_year")

# 9. missing values per column per year (shows what each era did and did not record)
skip = {"year", "month", "era", "hour"}
save(f.groupBy("year").agg(*[F.avg(F.col(c).isNull().cast("int")).alias(c)
                             for c in f.columns if c not in skip]).orderBy("year"), "nulls_year")

# 10. outliers and data errors per year
save(f.groupBy("year").agg(
        F.sum((F.col("arr_delay_min") > 720).cast("int")).alias("late_over_12h"),
        F.sum((F.col("arr_delay_min") < -90).cast("int")).alias("early_over_90min"),
        F.sum(((F.col("crs_elapsed_min") <= 0) | (F.col("crs_elapsed_min") > 900)).cast("int")).alias("bad_sched_duration"),
        F.sum((F.col("distance_mi") <= 0).cast("int")).alias("zero_distance"),
        F.sum(((F.col("crs_dep_hhmm") % 100 >= 60) | (F.col("crs_dep_hhmm") > 2400)).cast("int")).alias("bad_dep_time"),
        F.sum((F.col("origin") == F.col("dest")).cast("int")).alias("same_origin_dest"))
      .orderBy("year"), "outliers_year")
dup = (f.groupBy("year", "flight_date", "carrier", "flight_num", "origin", "crs_dep_hhmm").count()
        .filter("count > 1").groupBy("year").agg(F.count("*").alias("dup_groups"),
                                                 F.sum("count").alias("dup_rows")))
save(dup.orderBy("year"), "duplicates_year")

# 11. weather effects, by era (is the effect stable over 38 years?)
BINS = {
    "visibility_km": [0, 1, 3, 5, 10, 1e9], "wind_speed_ms": [0, 3, 6, 9, 12, 1e9],
    "temp_c": [-1e9, -10, 0, 10, 20, 30, 1e9], "precip_mm": [0, 1e-9, 1, 1e9],
    "ceiling_m": [0, 150, 300, 1000, 3000, 1e9],
}
parts = []
for col, edges in BINS.items():
    c = F.col(f"o_{col}")
    b = None
    for k, (lo, hi) in enumerate(zip(edges[:-1], edges[1:])):
        cond = (c >= lo) & (c < hi)
        b = F.when(cond, k) if b is None else b.when(cond, k)
    parts.append(op.filter(c.isNotNull()).groupBy("era", b.alias("bin")).agg(n, late)
                   .withColumn("variable", F.lit(col)))
wx = parts[0]
for p in parts[1:]:
    wx = wx.unionByName(p)
save(wx.orderBy("variable", "era", "bin"), "weather_bins_era")
save(op.filter(F.col("o_pres_wx").isNotNull()).groupBy("o_pres_wx").agg(n, late)
       .orderBy(F.desc("n")).limit(60), "pres_wx_top")

# 12. arrival delay histogram (5-minute bins, clipped), by era
save(op.withColumn("bin", F.floor(F.least(F.greatest("arr_delay_min", F.lit(-60)), F.lit(300)) / 5).cast("int") * 5)
       .groupBy("era", "bin").agg(n).orderBy("era", "bin"), "delay_hist_era")
