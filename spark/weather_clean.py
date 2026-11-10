"""Stage 2.2 - weather: NOAA GHCNh station-year Parquet (329 columns, several report types
per hour, per-field quality codes) -> one clean row per station per UTC hour.

Rules
  * hour: observation time rounded to the nearest hour (routine METARs are taken at :51)
  * per station-hour keep one report, preferring FM15 (routine METAR), then FM16 (special),
    then FM12 (SYNOP); latest within the hour wins
  * a value is dropped if its quality code is 2, 3, 6 or 7 (suspect/erroneous, ISD convention)
    or if it is physically implausible
Output: {base}/curated/weather_hourly/year=YYYY/
"""
from pyspark.sql import functions as F, Window
from common import parse_args, spark_session

args = parse_args(__doc__)
spark = spark_session("weather_clean")
base = args.base

wx_glob = f"{base}/weather/raw/2024/*.parquet" if args.mode == "sample" else f"{base}/weather/raw/*/*.parquet"
raw = spark.read.parquet(wx_glob)

BAD_QC = ["2", "3", "6", "7"]
# output name: (source column, lower bound, upper bound)
FIELDS = {
    "temp_c":          ("temperature",           -60, 60),
    "dewpoint_c":      ("dew_point_temperature", -70, 40),
    "rel_humidity":    ("relative_humidity",       0, 100),
    "wind_dir_deg":    ("wind_direction",          0, 360),
    "wind_speed_ms":   ("wind_speed",              0, 75),
    "wind_gust_ms":    ("wind_gust",               0, 100),
    "visibility_km":   ("visibility",              0, 200),
    "ceiling_m":       ("ceiling_height",          0, 30000),
    "precip_mm":       ("precipitation",           0, 300),
    "snow_depth_cm":   ("snow_depth",              0, 1000),
    "sea_level_hpa":   ("sea_level_pressure",    850, 1100),
}

def clean(src, lo, hi):
    v = F.col(src).cast("double")
    q = F.col(f"{src}_Quality_Code")
    ok = (q.isNull() | ~q.isin(BAD_QC)) & v.between(lo, hi)
    return F.when(ok, v)

ts = F.to_timestamp("DATE", "yyyy-MM-dd'T'HH:mm:ss")
rank = (F.when(F.col("report_type") == "FM15", 1)
         .when(F.col("report_type") == "FM16", 2)
         .when(F.col("report_type") == "FM12", 3).otherwise(9))

obs = raw.select(
    F.col("STATION").alias("station"),
    ts.alias("obs_utc"),
    F.date_trunc("hour", ts + F.expr("INTERVAL 30 MINUTES")).alias("hour_utc"),
    F.col("temperature_Report_Type").alias("report_type"),
    *[clean(src, lo, hi).alias(name) for name, (src, lo, hi) in FIELDS.items()],
    # present-weather code (thunder, snow, fog...): manual obs first, then automated
    F.coalesce("pres_wx_MW1", "pres_wx_AU1", "pres_wx_AW1").alias("pres_wx"),
).filter(F.col("obs_utc").isNotNull())

w = Window.partitionBy("station", "hour_utc").orderBy(rank.asc(), F.col("obs_utc").desc())
hourly = (obs.withColumn("rk", F.row_number().over(w)).filter("rk = 1").drop("rk")
          .withColumn("year", F.year("hour_utc")))

(hourly.repartition("year").write.mode("overwrite").partitionBy("year")
 .parquet(f"{base}/curated/weather_hourly"))

out = spark.read.parquet(f"{base}/curated/weather_hourly")
print(f"raw observations: {raw.count():,}   station-hours: {out.count():,}")
out.groupBy("report_type").count().orderBy("report_type").show()
