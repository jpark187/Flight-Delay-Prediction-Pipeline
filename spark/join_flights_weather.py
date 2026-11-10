"""Stage 2.3 - join every flight to the weather at its origin and destination airports from
the latest hourly report taken BEFORE scheduled departure (no information from after it).

BTS scheduled times are local clock times; weather is UTC. Each scheduled departure is
converted to UTC with the origin airport's IANA time zone (handles daylight saving).
Airport -> weather station comes from the yearly manifest written by load_weather.sh.

Output: {base}/curated/flights_wx/year=YYYY/month=M/
        {base}/curated/checks/join_coverage/   share of flights with weather, by year
"""
from pyspark.sql import functions as F
from common import parse_args, spark_session

args = parse_args(__doc__)
spark = spark_session("join_flights_weather")
base = args.base

flights = spark.read.parquet(f"{base}/curated/flights")
weather = spark.read.parquet(f"{base}/curated/weather_hourly")
tz = (spark.read.option("header", True).csv(f"{base}/reference/airport_tz.csv")
      .select(F.col("iata").alias("origin"), "tz"))
man_dir = "manifest_sample" if args.mode == "sample" else "manifest"
manifest = (spark.read.option("header", True).csv(f"{base}/weather/{man_dir}/*.csv")
            .filter(F.col("ghcn_id").isNotNull() & (F.col("ghcn_id") != ""))
            .select("iata", F.col("year").cast("int").alias("wx_year"),
                    F.col("ghcn_id").alias("station")))

# --- scheduled departure: local -> UTC. hhmm 2400 means midnight at the end of the day.
hhmm = F.col("crs_dep_hhmm")
local_dep = F.to_timestamp(F.concat_ws(" ",
    F.when(hhmm == 2400, F.date_add("flight_date", 1)).otherwise(F.col("flight_date")).cast("string"),
    F.lpad(F.when(hhmm == 2400, F.lit(0)).otherwise(hhmm).cast("string"), 4, "0")),
    "yyyy-MM-dd HHmm")

f = (flights.join(F.broadcast(tz), "origin", "left")
     .withColumn("crs_dep_local", local_dep)
     .withColumn("crs_dep_utc", F.to_utc_timestamp("crs_dep_local", F.col("tz")))
     # latest weather hour whose observations (taken up to 30 min after the hour mark,
     # see weather_clean.py) are all before the scheduled departure: floor(dep - 30 min)
     .withColumn("wx_hour_utc", F.date_trunc("hour", F.col("crs_dep_utc") - F.expr("INTERVAL 30 MINUTES")))
     .withColumn("wx_year", F.year("wx_hour_utc")))

WX = ["temp_c", "dewpoint_c", "rel_humidity", "wind_dir_deg", "wind_speed_ms", "wind_gust_ms",
      "visibility_km", "ceiling_m", "precip_mm", "snow_depth_cm", "sea_level_hpa", "pres_wx"]

def attach(df, airport_col, prefix):
    m = F.broadcast(manifest.withColumnRenamed("iata", airport_col)
                    .withColumnRenamed("station", f"{prefix}station"))
    wx = weather.select(F.col("station").alias(f"{prefix}station"),
                        F.col("hour_utc").alias("wx_hour_utc"),
                        *[F.col(c).alias(prefix + c) for c in WX])
    return (df.join(m, [airport_col, "wx_year"], "left")
              .join(wx, [f"{prefix}station", "wx_hour_utc"], "left"))

out = attach(attach(f, "origin", "o_"), "dest", "d_")

(out.repartition("year", "month").write.mode("overwrite").partitionBy("year", "month")
 .parquet(f"{base}/curated/flights_wx"))

res = spark.read.parquet(f"{base}/curated/flights_wx")
cov = (res.groupBy("year").agg(
        F.count("*").alias("flights"),
        F.avg(F.col("tz").isNull().cast("int")).alias("no_timezone"),
        F.avg(F.col("o_station").isNotNull().cast("int")).alias("origin_station"),
        F.avg(F.col("o_temp_c").isNotNull().cast("int")).alias("origin_wx"),
        F.avg(F.col("d_temp_c").isNotNull().cast("int")).alias("dest_wx"))
       .orderBy("year"))
cov.coalesce(1).write.mode("overwrite").option("header", True).csv(f"{base}/curated/checks/join_coverage")
cov.show(50, truncate=False)
