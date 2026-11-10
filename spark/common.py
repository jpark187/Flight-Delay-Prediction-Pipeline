"""Shared helpers for the Stage 2 Spark jobs."""
import argparse
from pyspark.sql import SparkSession


def parse_args(desc):
    p = argparse.ArgumentParser(description=desc)
    p.add_argument("--base", required=True, help="data lake root, e.g. gs://my-bucket")
    p.add_argument("--mode", choices=["sample", "full"], default="sample",
                   help="sample = 2024 only; full = Oct 1987 - Dec 2025")
    return p.parse_args()


def spark_session(name):
    return (SparkSession.builder.appName(name)
            .config("spark.sql.session.timeZone", "UTC")          # all timestamps are UTC
            .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
            .getOrCreate())
