"""Stage 5.1 - build the documents MongoDB serves, plus two correctness checks.

Spark does the heavy work over the full history and writes small JSON-lines files to GCS;
scripts/load_mongo.py loads them into Atlas from Cloud Shell (the cluster never needs
database credentials).

Collections (one JSON-lines folder each under {base}/serving/)
  routes       one per origin-dest route flown in the last year: the exact 30- and 365-day counts
               the model's route features need, by-hour late rates, carriers on the route, typical
               distance and duration
  airports     one per airport: 30-day departure and arrival counts, 30-day counts by departure
               hour, late rate for every year 1987-2025, time zone and weather station
  carriers     one per carrier: 30-day counts and yearly late rates
  route_months one per route per month, 2018-2025 (route history charts; MQL 'ask it twice')
Checks (under {base}/curated/checks/)
  ask_twice_spark   ORD's 2024 late rate computed with Spark SQL
  bts_monthly       monthly on-time / delayed / cancelled / diverted shares, to compare with BTS
Definitions match build_features.py: operated flights with a known outcome; history windows end
the day before 'as_of' + 1, i.e. they include the last day of data.
"""
from pyspark.sql import functions as F
from common import parse_args, spark_session

args = parse_args(__doc__)
spark = spark_session("build_serving_docs")
base = args.base

flights = spark.read.parquet(f"{base}/curated/flights")
flights.createOrReplaceTempView("flights")
hour = F.when(F.col("crs_dep_hhmm") == 2400, 0).otherwise((F.col("crs_dep_hhmm") / 100).cast("int"))
op = (flights.filter((F.col("cancelled") == 0) & F.col("arr_del15").isNotNull())
             .filter(F.col("crs_elapsed_min").between(1, 900) & (F.col("distance_mi") > 0)
                     & (F.col("origin") != F.col("dest")))
             .dropDuplicates(["flight_date", "carrier", "flight_num", "origin", "crs_dep_hhmm"])
             .withColumn("dep_hour", hour))
as_of = op.agg(F.max("flight_date")).first()[0]
print("as_of", as_of)
in_days = lambda d: F.col("flight_date") > F.date_sub(F.lit(as_of), d)    # last d days, incl. as_of

def save(df, name):
    df.coalesce(1).write.mode("overwrite").json(f"{base}/serving/{name}")
    print("wrote", name, df.count())

# ---------------------------------------------------------------- routes
last365 = op.filter(in_days(365))
r365 = last365.groupBy("origin", "dest").agg(
         F.count("*").alias("n365"), F.sum("arr_del15").cast("long").alias("late365"),
         F.expr("percentile_approx(distance_mi, 0.5)").alias("distance_mi"),
         F.expr("percentile_approx(crs_elapsed_min, 0.5)").alias("crs_elapsed_min"))
r30 = op.filter(in_days(30)).groupBy("origin", "dest").agg(
        F.count("*").alias("n30"), F.sum("arr_del15").cast("long").alias("late30"))
rhour = (last365.groupBy("origin", "dest", "dep_hour")
         .agg(F.count("*").alias("n"), F.avg("arr_del15").alias("late_rate"))
         .groupBy("origin", "dest").agg(F.sort_array(F.collect_list(
             F.struct("dep_hour", "n", F.round("late_rate", 4).alias("late_rate")))).alias("by_hour")))
rcarr = (last365.groupBy("origin", "dest", "carrier")
         .agg(F.count("*").alias("n"), F.avg("arr_del15").alias("late_rate"))
         .groupBy("origin", "dest").agg(F.collect_list(
             F.struct("carrier", "n", F.round("late_rate", 4).alias("late_rate"))).alias("carriers")))
routes = (r365.join(r30, ["origin", "dest"], "left").join(rhour, ["origin", "dest"], "left")
          .join(rcarr, ["origin", "dest"], "left")
          .fillna(0, ["n30", "late30"])
          .withColumn("_id", F.concat_ws("-", "origin", "dest"))
          .withColumn("as_of", F.lit(str(as_of))))
save(routes, "routes")

# ---------------------------------------------------------------- airports
tz = spark.read.option("header", True).csv(f"{base}/reference/airport_tz.csv")
man = (spark.read.option("header", True).csv(f"{base}/weather/manifest/*.csv")
       .filter(F.col("ghcn_id") != "").groupBy("iata").agg(F.max(F.struct("year", "ghcn_id")).alias("m"))
       .select(F.col("iata"), F.col("m.ghcn_id").alias("weather_station")))
last30 = op.filter(in_days(30))
dep30 = last30.groupBy(F.col("origin").alias("iata")).agg(
          F.count("*").alias("dep_n30"), F.sum("arr_del15").cast("long").alias("dep_late30"))
arr30 = last30.groupBy(F.col("dest").alias("iata")).agg(
          F.count("*").alias("arr_n30"), F.sum("arr_del15").cast("long").alias("arr_late30"))
hr30 = (last30.groupBy(F.col("origin").alias("iata"), "dep_hour")
        .agg(F.count("*").alias("n"), F.sum("arr_del15").cast("long").alias("late"))
        .groupBy("iata").agg(F.sort_array(F.collect_list(F.struct("dep_hour", "n", "late"))).alias("dep_by_hour30")))
yearly = (op.groupBy(F.col("origin").alias("iata"), "year")
          .agg(F.count("*").alias("n"), F.round(F.avg("arr_del15"), 4).alias("late_rate"))
          .groupBy("iata").agg(F.sort_array(F.collect_list(F.struct("year", "n", "late_rate"))).alias("yearly")))
airports = (yearly.join(dep30, "iata", "left").join(arr30, "iata", "left").join(hr30, "iata", "left")
            .join(tz.select("iata", "tz", "name", F.col("lat").cast("double"), F.col("lon").cast("double")), "iata", "left")
            .join(man, "iata", "left")
            .fillna(0, ["dep_n30", "dep_late30", "arr_n30", "arr_late30"])
            .withColumn("_id", F.col("iata")).withColumn("as_of", F.lit(str(as_of))))
save(airports, "airports")

# ---------------------------------------------------------------- carriers
c30 = last30.groupBy("carrier").agg(F.count("*").alias("n30"), F.sum("arr_del15").cast("long").alias("late30"))
cyear = (op.groupBy("carrier", "year").agg(F.count("*").alias("n"), F.round(F.avg("arr_del15"), 4).alias("late_rate"))
         .groupBy("carrier").agg(F.sort_array(F.collect_list(F.struct("year", "n", "late_rate"))).alias("yearly")))
save(cyear.join(c30, "carrier", "left").fillna(0, ["n30", "late30"])
          .withColumn("_id", F.col("carrier")).withColumn("as_of", F.lit(str(as_of))), "carriers")

# ---------------------------------------------------------------- route_months (simple definition,
# shared with the 'ask it twice' check: operated flights with a known outcome, no other filters)
simple = flights.filter((F.col("cancelled") == 0) & F.col("arr_del15").isNotNull())
save(simple.filter(F.col("year") >= 2018).groupBy("origin", "dest", "year", "month")
           .agg(F.count("*").alias("n"), F.sum("arr_del15").cast("long").alias("late")), "route_months")

# ---------------------------------------------------------------- checks
ask = spark.sql("""
    SELECT 'spark_sql' AS method, COUNT(*) AS flights, SUM(arr_del15) AS late,
           SUM(arr_del15) / COUNT(*) AS late_rate
    FROM flights
    WHERE origin = 'ORD' AND year = 2024 AND cancelled = 0 AND arr_del15 IS NOT NULL""")
ask.coalesce(1).write.mode("overwrite").option("header", True).csv(f"{base}/curated/checks/ask_twice_spark")
ask.show()

# BTS reports shares of scheduled flights: on time + delayed (15+) + cancelled + diverted = 100%
bts = (flights.filter(F.col("year") >= 2023).groupBy("year", "month").agg(
        F.count("*").alias("scheduled"),
        F.avg(F.coalesce((F.col("cancelled") == 0) & (F.col("diverted") == 0) & (F.col("arr_del15") == 0),
                         F.lit(False)).cast("int")).alias("on_time"),
        F.avg(F.coalesce(F.col("arr_del15") == 1, F.lit(False)).cast("int")).alias("delayed"),
        F.avg("cancelled").alias("cancelled"), F.avg("diverted").alias("diverted")).orderBy("year", "month"))
bts.coalesce(1).write.mode("overwrite").option("header", True).csv(f"{base}/curated/checks/bts_monthly")
bts.show(36)
