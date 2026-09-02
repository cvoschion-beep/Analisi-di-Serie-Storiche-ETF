"""
ETF Analytics Pipeline — Glue Job 1: Raw → Staging
----------------------------------------------------
Reads raw Parquet files from S3, applies data quality transformations,
computes basic indicators, and writes clean data to the staging area.

How to configure in AWS Glue:
  - Type       : Spark (Glue 4.0, Python 3)
  - IAM Role   : LabRole
  - Job param  : --S3_BUCKET  etf-analytics-pipeline-2025
"""

import sys
from datetime import date, timedelta

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.sql.types import DateType, DoubleType, LongType, StringType

# Fix compatibilità pyarrow timestamp
import pyarrow as pa

# ── Init Glue + Spark ─────────────────────────────────────────────────────────
args = getResolvedOptions(sys.argv, ["JOB_NAME", "S3_BUCKET"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

S3_BUCKET = args["S3_BUCKET"]
RAW_PATH     = f"s3://{S3_BUCKET}/raw/"
STAGING_PATH = f"s3://{S3_BUCKET}/staging/"

# ── 1. LETTURA dati raw ───────────────────────────────────────────────────────
print(">>> [1/7] Reading raw data from S3...")

df = spark.read \
          .option("mergeSchema", "true") \
          .option("spark.sql.parquet.int96RebaseModeInRead", "CORRECTED") \
          .option("spark.sql.parquet.datetimeRebaseModeInRead", "CORRECTED") \
          .option("basePath", RAW_PATH) \
          .parquet(RAW_PATH)

print(f"    Raw rows loaded: {df.count()}")
print(f"    Schema:")
df.printSchema()


# ── 2. CAST tipi di dato ──────────────────────────────────────────────────────
# yfinance a volte restituisce tipi inconsistenti → forziamo i tipi corretti
print(">>> [2/7] Casting data types...")

df = (
    df
    .withColumn("date",           F.col("date").cast(DateType()))
    .withColumn("open",           F.col("open").cast(DoubleType()))
    .withColumn("high",           F.col("high").cast(DoubleType()))
    .withColumn("low",            F.col("low").cast(DoubleType()))
    .withColumn("close",          F.col("close").cast(DoubleType()))
    .withColumn("volume",         F.col("volume").cast(LongType()))
    .withColumn("ticker",         F.col("ticker").cast(StringType()))
    .withColumn("ingestion_date", F.col("ingestion_date").cast(StringType()))
)


# ── 3. DEDUPLICAZIONE ─────────────────────────────────────────────────────────
# Ogni run Lambda scarica 5 giorni → possibili duplicati tra run consecutive
print(">>> [3/7] Removing duplicates...")

rows_before = df.count()
df = df.dropDuplicates(["date", "ticker"])
rows_after = df.count()

print(f"    Duplicates removed: {rows_before - rows_after}")


# ── 4. MISSING VALUES ────────────────────────────────────────────────────────
# Semplificato: forward-fill diretto senza cross join
print(">>> [4/7] Handling missing values...")

window_ffill = (
    Window
    .partitionBy("ticker")
    .orderBy("date")
    .rowsBetween(Window.unboundedPreceding, 0)
)

for col_name in ["open", "high", "low", "close", "volume"]:
    df = df.withColumn(
        col_name,
        F.last(F.col(col_name), ignorenulls=True).over(window_ffill)
    )

df = df.dropna(subset=["close"])
print(f"    Rows after fill: {df.count()}")


# ── 5. OUTLIER DETECTION sui volumi ──────────────────────────────────────────
# Usa Z-score per ticker: |z| > 3 → volume anomalo
# Non rimuoviamo, solo flagghiamo (per audit trail)
print(">>> [5/7] Flagging volume outliers...")

window_stats = Window.partitionBy("ticker")

df = (
    df
    .withColumn("vol_mean", F.mean("volume").over(window_stats))
    .withColumn("vol_std",  F.stddev("volume").over(window_stats))
    .withColumn(
        "volume_zscore",
        (F.col("volume") - F.col("vol_mean")) / F.col("vol_std")
    )
    .withColumn(
        "is_volume_outlier",
        F.abs(F.col("volume_zscore")) > 3
    )
    .drop("vol_mean", "vol_std")   # pulizia colonne temporanee
)

outlier_count = df.filter(F.col("is_volume_outlier")).count()
print(f"    Volume outliers flagged: {outlier_count}")


# ── 6. FEATURE ENGINEERING base ──────────────────────────────────────────────
print(">>> [6/7] Computing base indicators...")

window_ordered = Window.partitionBy("ticker").orderBy("date")

# Rendimento giornaliero percentuale
# formula: (close_oggi - close_ieri) / close_ieri
df = df.withColumn(
    "daily_return",
    (F.col("close") - F.lag("close", 1).over(window_ordered))
    / F.lag("close", 1).over(window_ordered)
)

# Z-score del rendimento giornaliero (per anomaly detection)
df = (
    df
    .withColumn("ret_mean", F.mean("daily_return").over(window_stats))
    .withColumn("ret_std",  F.stddev("daily_return").over(window_stats))
    .withColumn(
        "return_zscore",
        (F.col("daily_return") - F.col("ret_mean")) / F.col("ret_std")
    )
    .drop("ret_mean", "ret_std")
)

# Prezzo normalizzato base 100
# Ogni ETF parte da 100 → confronto visivo diretto tra ETF con prezzi diversi
first_close = (
    df
    .groupBy("ticker")
    .agg(F.first("close", ignorenulls=True).alias("first_close"))
)

df = df.join(first_close, on="ticker", how="left")
df = df.withColumn(
    "close_norm",
    (F.col("close") / F.col("first_close")) * 100
).drop("first_close")


# ── 7. SCRITTURA staging ──────────────────────────────────────────────────────
print(">>> [7/7] Writing to staging...")

(
    df
    .repartition("ticker")          # un file per ticker → query più efficienti
    .write
    .mode("overwrite")              # riscrive tutto ad ogni run
    .partitionBy("ticker")          # partitioning su S3
    .parquet(STAGING_PATH)
)

print(f"    Staging written to: {STAGING_PATH}")
print(f"    Final schema:")
df.printSchema()

job.commit()
print(">>> Job 1 completed successfully.")
