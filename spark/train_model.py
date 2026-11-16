"""Stage 4.2 - train and evaluate the delay model (Spark MLlib), and export it for serving.

Models
  baseline     the route's 30-day late rate, used directly as the predicted probability
  no_weather   logistic regression on schedule, history and rotation features
  full         logistic regression on all features (adds origin and destination weather)
Train on the 'train' split (2018, 2019, 2021-2023), compare on 'valid' (2024), report 'test'
(2025) and 'covid' (2020) once. A random TRAIN_FRACTION of training rows is used: logistic
regression with ~100 inputs stops improving long before 33M rows, and the sample fits in memory.

Outputs
  {base}/model/model.json                  everything the API needs to score a flight (no Spark)
  {base}/curated/checks/model_metrics/     AUC, PR-AUC, log loss, Brier score per model and split
  {base}/curated/checks/calibration/       predicted vs actual late rate by risk decile
  {base}/curated/checks/segments/          AUC on thunderstorm, snow, evening and first-leg flights
  {base}/curated/checks/coefficients/      standardized coefficients of the full model
"""
import json, math, os
from pyspark.sql import functions as F
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.functions import vector_to_array
from common import parse_args, spark_session

args = parse_args(__doc__)
spark = spark_session("train_model")
base = args.base
TRAIN_FRACTION = float(os.environ.get("TRAIN_FRACTION", "0.3"))
SEED = 42

df = spark.read.parquet(f"{base}/curated/features")

# ---------------------------------------------------------------- inputs the API can rebuild exactly
carriers = [r.carrier for r in (df.filter("split = 'train'").groupBy("carrier").count()
                                .filter("count >= 10000").orderBy("carrier").collect())]
HOURS = list(range(5, 24))                              # 0-4 grouped as 'early' (few flights)

def prepare(d):
    d = (d.withColumn("turn_known", F.col("turn_min").isNotNull().cast("int"))
          .withColumn("turn_c", F.least(F.coalesce("turn_min", F.lit(300.0)), F.lit(300.0)))
          .withColumn("turn_short", ((F.col("turn_min") < 45) & F.col("turn_min").isNotNull()).cast("int"))
          .withColumn("legs_c", F.least(F.coalesce("legs_before", F.lit(0)), F.lit(8)).cast("double"))
          .withColumn("log_distance", F.log1p("distance_mi"))
          .withColumn("h_early", (F.col("dep_hour") < 5).cast("int")))
    for h in HOURS:
        d = d.withColumn(f"h_{h}", (F.col("dep_hour") == h).cast("int"))
    for m in range(1, 13):
        d = d.withColumn(f"m_{m}", (F.col("month") == m).cast("int"))
    for w in range(1, 8):
        d = d.withColumn(f"dow_{w}", (F.col("day_of_week") == w).cast("int"))
    for c in carriers:
        d = d.withColumn(f"c_{c}", (F.col("carrier") == c).cast("int"))
    return d

CAT = (["h_early"] + [f"h_{h}" for h in HOURS] + [f"m_{m}" for m in range(1, 13)]
       + [f"dow_{w}" for w in range(1, 8)] + [f"c_{c}" for c in carriers])
NONWX = CAT + ["log_distance", "crs_elapsed_min", "legs_c", "turn_c", "turn_known", "turn_short",
               "tail_ok", "route30_rate", "route365_rate", "orig30_rate", "orighr30_rate",
               "dest30_rate", "carr30_rate", "route30_logn", "route365_logn"]
WXCOLS = ["wx_missing", "vis", "ceil_low", "ceil_km", "wind", "gust", "precip", "precip_missing",
          "temp", "hot", "freezing_temp", "wx_thunder", "wx_snow", "wx_freezing", "wx_rain",
          "wx_showers", "wx_fog", "wx_mist_haze", "wx_heavy", "wx_light"]
FULL = NONWX + [p + c for p in ("o_", "d_") for c in WXCOLS]

data = prepare(df).withColumn("label", F.col("label").cast("double"))
for c in FULL:
    data = data.withColumn(c, F.col(c).cast("double"))
# Weather, rates and turnaround already carry explicit defaults (build_features.py / prepare);
# anything still null (e.g. an hour flag for a flight without a local time) becomes 0 = "not set".
data = data.na.fill(0.0, subset=FULL)
train = (data.filter("split = 'train'").sample(fraction=TRAIN_FRACTION, seed=SEED)
         .select("label", *FULL).cache())
n_train = train.count()
print(f"training rows: {n_train:,} ({TRAIN_FRACTION:.0%} sample)")

def fit(cols, name):
    pipe = Pipeline(stages=[
        VectorAssembler(inputCols=cols, outputCol="raw", handleInvalid="keep"),
        StandardScaler(inputCol="raw", outputCol="features", withMean=True, withStd=True),
        LogisticRegression(featuresCol="features", labelCol="label", maxIter=100,
                           regParam=1e-4, elasticNetParam=0.0, standardization=False)])
    model = pipe.fit(train)
    print(f"fitted {name}: {model.stages[-1].summary.totalIterations} iterations")
    return model

models = {"no_weather": (fit(NONWX, "no_weather"), NONWX), "full": (fit(FULL, "full"), FULL)}

# ---------------------------------------------------------------- evaluation
auc = BinaryClassificationEvaluator(labelCol="label", rawPredictionCol="score", metricName="areaUnderROC")
pr = BinaryClassificationEvaluator(labelCol="label", rawPredictionCol="score", metricName="areaUnderPR")
EPS = 1e-6
def scored(name, split):
    d = data.filter(F.col("split") == split)
    if name == "baseline":
        return d.withColumn("p", F.col("route30_rate")).withColumn("score", F.col("p"))
    m, _ = models[name]
    return (m.transform(d).withColumn("p", vector_to_array("probability")[1])
             .withColumn("score", F.col("p")))

metrics, calib, segs = [], [], []
for split in ["valid", "test", "covid"]:
    for name in ["baseline", "no_weather", "full"]:
        s = scored(name, split).select("label", "p", "score", "o_wx_thunder", "o_wx_snow",
                                       "dep_hour", "turn_known").cache()
        pc = F.least(F.greatest("p", F.lit(EPS)), F.lit(1 - EPS))
        agg = s.agg(F.count("*").alias("n"), F.avg("label").alias("late_rate"),
                    F.avg("p").alias("mean_pred"),
                    F.avg(-(F.col("label") * F.log(pc) + (1 - F.col("label")) * F.log(1 - pc))).alias("logloss"),
                    F.avg((F.col("p") - F.col("label")) ** 2).alias("brier")).first()
        row = dict(model=name, split=split, auc_roc=auc.evaluate(s), auc_pr=pr.evaluate(s), **agg.asDict())
        metrics.append(row); print(row)
        if split in ("valid", "test"):
            dec = (s.withColumn("decile", F.least((F.col("p") * 10).cast("int"), F.lit(9)))
                    .groupBy("decile").agg(F.count("*").alias("n"), F.avg("p").alias("mean_pred"),
                                           F.avg("label").alias("actual")).orderBy("decile").collect())
            calib += [dict(model=name, split=split, **r.asDict()) for r in dec]
            for seg, cond in [("origin_thunder", "o_wx_thunder = 1"), ("origin_snow", "o_wx_snow = 1"),
                              ("evening_17_21", "dep_hour BETWEEN 17 AND 21"), ("first_leg", "turn_known = 0")]:
                g = s.filter(cond)
                st = g.agg(F.count("*").alias("n"), F.avg("label").alias("late_rate"),
                           F.avg("p").alias("mean_pred")).first()
                both = st.n > 100 and st.late_rate is not None and 0 < st.late_rate < 1
                segs.append(dict(model=name, split=split, segment=seg,
                                 auc_roc=auc.evaluate(g) if both else None, **st.asDict()))
        s.unpersist()

def write(rows, name):
    spark.createDataFrame(rows).coalesce(1).write.mode("overwrite").option("header", True) \
         .csv(f"{base}/curated/checks/{name}")
write(metrics, "model_metrics"); write(calib, "calibration"); write(segs, "segments")

# ---------------------------------------------------------------- export the full model for serving
m, cols = models["full"]
# 200 test flights with their prepared inputs and Spark's probability: the API's unit test checks
# that scoring from model.json reproduces these numbers exactly
(m.transform(data.filter("split = 'test'").limit(200))
   .withColumn("spark_p", vector_to_array("probability")[1])
   .select("flight_date", "carrier", "flight_num", "origin", "dest", *cols, "spark_p")
   .coalesce(1).write.mode("overwrite").option("header", True)
   .csv(f"{base}/curated/checks/scoring_sample"))
scaler, lr = m.stages[1], m.stages[2]
mean, std = scaler.mean.toArray().tolist(), scaler.std.toArray().tolist()
coef = lr.coefficients.toArray().tolist()
write([dict(feature=c, coef_standardized=float(b), abs_coef=abs(float(b))) for c, b in zip(cols, coef)],
      "coefficients")
export = {
    "model": "logistic regression (Spark MLlib), full feature set",
    "trained_on": "split=train (2018, 2019, 2021-2023)", "train_rows": n_train,
    "features": cols, "mean": mean, "std": [s if s > 0 else 1.0 for s in std],
    "coefficients": coef, "intercept": lr.intercept,
    "carriers": carriers, "hours": HOURS,
    "fill": {"any_remaining_null": 0.0, "turn_min": 300.0, "legs_cap": 8, "visibility_km": 16.0, "ceiling_m": 30000.0,
             "temp_c": 15.0, "global_late_rate": 0.20, "shrinkage_k": 20.0},
    "metrics": [r for r in metrics if r["model"] == "full"],
    "scoring": "z = (x - mean) / std; p = 1 / (1 + exp(-(intercept + coefficients . z)))",
}
text = json.dumps(export, indent=1, default=float)
# write one plain file (gs:// or local) through Hadoop's filesystem API, which Dataproc configures for GCS
jvm, conf = spark._jvm, spark._jsc.hadoopConfiguration()
path = jvm.org.apache.hadoop.fs.Path(f"{base}/model/model.json")
out = path.getFileSystem(conf).create(path, True)
out.write(bytearray(text.encode("utf-8"))); out.close()
print("exported model.json with", len(cols), "inputs")
