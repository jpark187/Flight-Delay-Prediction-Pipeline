"""Stage 2.1 - flights: BTS monthly CSVs (+ ASA Data Expo for months BTS no longer serves)
-> one harmonised table, Parquet partitioned by year/month.

Output:  {base}/curated/flights/year=YYYY/month=M/
         {base}/curated/checks/source_overlap/   BTS vs Data Expo, month by month
"""
from pyspark.sql import functions as F
from common import parse_args, spark_session

args = parse_args(__doc__)
spark = spark_session("flights_to_parquet")
base = args.base

d = lambda c: F.col(c).cast("double")
i = lambda c: F.col(c).cast("double").cast("int")

# ---------- BTS (110 columns, all read as strings, then cast) ----------
# full: every year present (BTS has no 1990-1999); sample: 2024 only
bts_glob = f"{base}/raw/2024/*/*.csv" if args.mode == "sample" else f"{base}/raw/*/*/*.csv"
bts_raw = spark.read.option("header", True).option("inferSchema", False).csv(bts_glob)
bts = bts_raw.select(
    F.to_date("FlightDate").alias("flight_date"),
    i("Year").alias("year"), i("Month").alias("month"),
    i("DayofMonth").alias("day_of_month"), i("DayOfWeek").alias("day_of_week"),
    F.col("Reporting_Airline").alias("carrier"),
    i("Flight_Number_Reporting_Airline").alias("flight_num"),
    F.col("Tail_Number").alias("tail_num"),
    F.col("Origin").alias("origin"), F.col("Dest").alias("dest"),
    i("CRSDepTime").alias("crs_dep_hhmm"), i("CRSArrTime").alias("crs_arr_hhmm"),
    d("CRSElapsedTime").alias("crs_elapsed_min"), d("Distance").alias("distance_mi"),
    d("DepDelay").alias("dep_delay_min"), d("ArrDelay").alias("arr_delay_min"),
    i("ArrDel15").alias("arr_del15"),
    i("Cancelled").alias("cancelled"), i("Diverted").alias("diverted"),
    F.col("CancellationCode").alias("cancellation_code"),
    d("CarrierDelay").alias("carrier_delay_min"), d("WeatherDelay").alias("weather_delay_min"),
    d("NASDelay").alias("nas_delay_min"), d("SecurityDelay").alias("security_delay_min"),
    d("LateAircraftDelay").alias("late_aircraft_delay_min"),
    F.lit("bts").alias("source"),
)

# ---------- ASA Data Expo (29 columns, 'NA' = null) ----------
expo = None
if args.mode == "full":
    expo_raw = (spark.read.option("header", True).option("inferSchema", False)
                .option("nullValue", "NA").csv(f"{base}/raw_expo/*.csv.bz2"))
    arr = d("ArrDelay")
    expo = expo_raw.select(
        F.make_date(i("Year"), i("Month"), i("DayofMonth")).alias("flight_date"),
        i("Year").alias("year"), i("Month").alias("month"),
        i("DayofMonth").alias("day_of_month"), i("DayOfWeek").alias("day_of_week"),
        F.col("UniqueCarrier").alias("carrier"), i("FlightNum").alias("flight_num"),
        F.col("TailNum").alias("tail_num"),
        F.col("Origin").alias("origin"), F.col("Dest").alias("dest"),
        i("CRSDepTime").alias("crs_dep_hhmm"), i("CRSArrTime").alias("crs_arr_hhmm"),
        d("CRSElapsedTime").alias("crs_elapsed_min"), d("Distance").alias("distance_mi"),
        d("DepDelay").alias("dep_delay_min"), arr.alias("arr_delay_min"),
        # Expo has no ArrDel15: same definition as BTS (null when cancelled/diverted)
        F.when(arr.isNull(), None).otherwise((arr >= 15).cast("int")).alias("arr_del15"),
        i("Cancelled").alias("cancelled"), i("Diverted").alias("diverted"),
        F.col("CancellationCode").alias("cancellation_code"),
        d("CarrierDelay").alias("carrier_delay_min"), d("WeatherDelay").alias("weather_delay_min"),
        d("NASDelay").alias("nas_delay_min"), d("SecurityDelay").alias("security_delay_min"),
        d("LateAircraftDelay").alias("late_aircraft_delay_min"),
        F.lit("expo").alias("source"),
    )

    # Check the two sources agree where both have the month.
    def monthly(df):
        return df.groupBy("year", "month").agg(
            F.count("*").alias("flights"),
            F.avg("arr_del15").alias("late_rate"),
            F.avg("cancelled").alias("cancel_rate"))
    b, e = monthly(bts).alias("b"), monthly(expo).alias("e")
    overlap = (b.join(e, ["year", "month"])
               .select("year", "month",
                       F.col("b.flights").alias("bts_flights"), F.col("e.flights").alias("expo_flights"),
                       (F.col("e.flights") / F.col("b.flights") - 1).alias("flights_rel_diff"),
                       F.col("b.late_rate").alias("bts_late_rate"), F.col("e.late_rate").alias("expo_late_rate"),
                       F.col("b.cancel_rate").alias("bts_cancel_rate"),
                       F.col("e.cancel_rate").alias("expo_cancel_rate"))
               .orderBy("year", "month"))
    overlap.coalesce(1).write.mode("overwrite").option("header", True) \
        .csv(f"{base}/curated/checks/source_overlap")
    print("source overlap (first rows):"); overlap.show(12, truncate=False)

    # Use Expo only for months BTS does not have.
    bts_months = bts.select("year", "month").distinct()
    expo = expo.join(F.broadcast(bts_months), ["year", "month"], "left_anti")

flights = bts if expo is None else bts.unionByName(expo)
flights = flights.filter(F.col("flight_date").isNotNull())

(flights.repartition("year", "month")
 .write.mode("overwrite").partitionBy("year", "month")
 .parquet(f"{base}/curated/flights"))

summary = (spark.read.parquet(f"{base}/curated/flights")
           .groupBy("source").agg(F.count("*").alias("rows"),
                                  F.min("flight_date").alias("first"),
                                  F.max("flight_date").alias("last")))
summary.show(truncate=False)
