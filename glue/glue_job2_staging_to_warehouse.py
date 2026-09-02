"""
ETF Analytics Pipeline — Glue Job 2: Staging → Warehouse
----------------------------------------------------------
Reads clean data from staging, builds the 4 DWH tables:
  1. dim_etf          → ETF registry (slowly changing dimension)
  2. fact_prices      → daily OHLCV + base indicators
  3. fact_rolling     → rolling statistics (SMA, volatility, drawdown)
  4. fact_anomalies   → statistically anomalous days

How to configure in AWS Glue:
  - Type       : Spark (Glue 4.0, Python 3)
  - IAM Role   : LabRole
  - Job param  : --S3_BUCKET  etf-analytics-pipeline-2025
"""

import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

# ── Init Glue + Spark ─────────────────────────────────────────────────────────
args = getResolvedOptions(sys.argv, ["JOB_NAME", "S3_BUCKET"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

S3_BUCKET      = args["S3_BUCKET"]
STAGING_PATH   = f"s3://{S3_BUCKET}/staging/"
WAREHOUSE_PATH = f"s3://{S3_BUCKET}/warehouse/"

# ── Lettura staging ───────────────────────────────────────────────────────────
print(">>> Reading staging data...")
df = spark.read.parquet(STAGING_PATH)
print(f"    Rows loaded: {df.count()}")

# Window functions riutilizzate in più tabelle
window_ordered = Window.partitionBy("ticker").orderBy("date")
window_stats   = Window.partitionBy("ticker")


# ════════════════════════════════════════════════════════════════════════════════
# TABELLA 1 — dim_etf
# Anagrafica statica degli ETF: una riga per ticker
# In un DWH reale questa sarebbe una SCD (Slowly Changing Dimension)
# ════════════════════════════════════════════════════════════════════════════════
print(">>> [1/4] Building dim_etf...")

# Mappa arricchita con info che yfinance non fornisce
etf_metadata = spark.createDataFrame([
    ("SWRD.L", "SPDR MSCI World UCITS ETF",                  "State Street", "Equity Sviluppati", "MSCI World",          "USD", "LSE"),
    ("SPY",    "SPDR S&P 500 ETF Trust",                     "State Street", "Equity Sviluppati", "S&P 500",             "USD", "NYSE"),
    ("QQQ",    "Invesco Nasdaq 100 ETF",                      "Invesco",      "Equity Sviluppati", "Nasdaq 100",          "USD", "NYSE"),
    ("EFA",    "iShares MSCI EAFE ETF",                       "BlackRock",    "Equity Sviluppati", "MSCI EAFE",           "USD", "NYSE"),
    ("VEUR.L", "Vanguard FTSE Developed Europe UCITS ETF",    "Vanguard",     "Equity Sviluppati", "FTSE Developed Europe","USD","LSE"),
    ("CSPX.L", "iShares Core S&P 500 UCITS ETF",             "BlackRock",    "Equity Sviluppati", "S&P 500",             "USD", "LSE"),
    ("EIMI.L", "iShares Core MSCI EM IMI UCITS ETF",         "BlackRock",    "Equity Emergenti",  "MSCI EM IMI",         "USD", "LSE"),
    ("VFEM.L", "Vanguard FTSE Emerging Markets UCITS ETF",   "Vanguard",     "Equity Emergenti",  "FTSE Emerging Markets","USD","LSE"),
    ("TLT",    "iShares 20+ Year Treasury Bond ETF",         "BlackRock",    "Obbligazionario",   "ICE US Treasury 20+", "USD", "NYSE"),
    ("AGG",    "iShares Core US Aggregate Bond ETF",         "BlackRock",    "Obbligazionario",   "Bloomberg US Agg",    "USD", "NYSE"),
    ("HYG",    "iShares iBoxx High Yield Corporate Bond ETF","BlackRock",    "Obbligazionario",   "iBoxx USD Liquid HY", "USD", "NYSE"),
    ("LQD",    "iShares Investment Grade Corporate Bond ETF","BlackRock",    "Obbligazionario",   "iBoxx USD IG Corp",   "USD", "NYSE"),
    ("GLD",    "SPDR Gold Shares",                           "State Street", "Commodity",         "Gold Spot",           "USD", "NYSE"),
    ("SLV",    "iShares Silver Trust",                       "BlackRock",    "Commodity",         "Silver Spot",         "USD", "NYSE"),
    ("USO",    "United States Oil Fund",                     "USCF",         "Commodity",         "WTI Crude Oil",       "USD", "NYSE"),
    ("PDBC",   "Invesco Optimum Yield Diversified Commodity","Invesco",      "Commodity",         "DBIQ Commodity Index","USD", "NYSE"),
    ("XLK",    "Technology Select Sector SPDR Fund",         "State Street", "Settoriale",        "S&P Tech Sector",     "USD", "NYSE"),
    ("XLV",    "Health Care Select Sector SPDR Fund",        "State Street", "Settoriale",        "S&P Health Sector",   "USD", "NYSE"),
    ("XLE",    "Energy Select Sector SPDR Fund",             "State Street", "Settoriale",        "S&P Energy Sector",   "USD", "NYSE"),
    ("ARKK",   "ARK Innovation ETF",                         "ARK Invest",   "Settoriale",        "ARK Innovation",      "USD", "NYSE"),
], ["ticker", "etf_name", "provider", "category", "index_tracked", "currency", "exchange"])

# Aggiungi statistiche descrittive calcolate dai dati reali
stats = df.groupBy("ticker").agg(
    F.min("date").alias("first_date"),
    F.max("date").alias("last_date"),
    F.count("date").alias("trading_days"),
    F.round(F.avg("volume"), 0).alias("avg_daily_volume"),
)

dim_etf = etf_metadata.join(stats, on="ticker", how="left")

(
    dim_etf
    .write
    .mode("overwrite")
    .parquet(f"{WAREHOUSE_PATH}dim_etf/")
)
print(f"    dim_etf written — {dim_etf.count()} rows")


# ════════════════════════════════════════════════════════════════════════════════
# TABELLA 2 — fact_prices
# Serie storica giornaliera con indicatori base
# Questa è la tabella centrale del DWH — tutto si collega a lei
# ════════════════════════════════════════════════════════════════════════════════
print(">>> [2/4] Building fact_prices...")

fact_prices = df.select(
    "date",
    "ticker",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "close_norm",
    "daily_return",
    "return_zscore",
    "volume_zscore",
    "is_volume_outlier",
    "ingestion_date",
)

# Aggiunge variazione % rispetto al primo giorno (per confronto cumulativo)
fact_prices = fact_prices.withColumn(
    "cumulative_return",
    (F.col("close_norm") - 100) / 100   # es. 0.15 = +15% dal primo giorno
)

# Flag: giorno positivo o negativo
fact_prices = fact_prices.withColumn(
    "day_direction",
    F.when(F.col("daily_return") > 0, "positive")
     .when(F.col("daily_return") < 0, "negative")
     .otherwise("flat")
)

(
    fact_prices
    .repartition("ticker")
    .write
    .mode("overwrite")
    .partitionBy("ticker")
    .parquet(f"{WAREHOUSE_PATH}fact_prices/")
)
print(f"    fact_prices written — {fact_prices.count()} rows")


# ════════════════════════════════════════════════════════════════════════════════
# TABELLA 3 — fact_rolling
# Indicatori calcolati su finestre mobili (rolling)
# Cattura trend e volatilità nel tempo — fondamentale per i grafici
# ════════════════════════════════════════════════════════════════════════════════
print(">>> [3/4] Building fact_rolling...")

# Finestre mobili: ultimi N giorni inclusivi
window_20  = window_ordered.rowsBetween(-19, 0)   # 20 giorni
window_30  = window_ordered.rowsBetween(-29, 0)   # 30 giorni
window_252 = window_ordered.rowsBetween(-251, 0)  # ~1 anno lavorativo

fact_rolling = df.select("date", "ticker", "close", "daily_return", "close_norm")

# SMA — Simple Moving Average (media mobile prezzi)
fact_rolling = fact_rolling.withColumn("sma_20",  F.avg("close").over(window_20))
fact_rolling = fact_rolling.withColumn("sma_252", F.avg("close").over(window_252))

# Volatilità rolling = deviazione standard dei rendimenti giornalieri
# Annualizzata moltiplicando per sqrt(252) — convenzione finanziaria standard
fact_rolling = (
    fact_rolling
    .withColumn("volatility_30_daily", F.stddev("daily_return").over(window_30))
    .withColumn(
        "volatility_30_annualized",
        F.col("volatility_30_daily") * F.sqrt(F.lit(252))
    )
)

# Drawdown = quanto si è scesi rispetto al massimo storico raggiunto
# Mostra i "periodi di perdita" — molto leggibile visivamente
rolling_max = (
    Window
    .partitionBy("ticker")
    .orderBy("date")
    .rowsBetween(Window.unboundedPreceding, 0)
)
fact_rolling = fact_rolling.withColumn("rolling_max", F.max("close").over(rolling_max))
fact_rolling = fact_rolling.withColumn(
    "drawdown",
    (F.col("close") - F.col("rolling_max")) / F.col("rolling_max")
)

# Pulizia colonne temporanee
fact_rolling = fact_rolling.drop("rolling_max", "volatility_30_daily", "close")

(
    fact_rolling
    .repartition("ticker")
    .write
    .mode("overwrite")
    .partitionBy("ticker")
    .parquet(f"{WAREHOUSE_PATH}fact_rolling/")
)
print(f"    fact_rolling written — {fact_rolling.count()} rows")


# ════════════════════════════════════════════════════════════════════════════════
# TABELLA 4 — fact_anomalies
# Giorni statisticamente anomali nei rendimenti
# Un giorno è anomalo se il suo Z-score > 2.5 (roughly top/bottom 1.2%)
# ════════════════════════════════════════════════════════════════════════════════
print(">>> [4/4] Building fact_anomalies...")

ZSCORE_THRESHOLD = 2.5

fact_anomalies = (
    df
    .filter(F.abs(F.col("return_zscore")) > ZSCORE_THRESHOLD)
    .select(
        "date",
        "ticker",
        "close",
        "daily_return",
        "return_zscore",
        "volume",
        "volume_zscore",
    )
    .withColumn(
        "anomaly_type",
        F.when(F.col("daily_return") > 0, "spike_up")
         .otherwise("spike_down")
    )
    .withColumn(
        "anomaly_severity",
        F.when(F.abs(F.col("return_zscore")) > 4, "extreme")
         .when(F.abs(F.col("return_zscore")) > 3, "high")
         .otherwise("moderate")
    )
    .orderBy("ticker", "date")
)

(
    fact_anomalies
    .repartition("ticker")
    .write
    .mode("overwrite")
    .partitionBy("ticker")
    .parquet(f"{WAREHOUSE_PATH}fact_anomalies/")
)
print(f"    fact_anomalies written — {fact_anomalies.count()} rows")


# ── Riepilogo finale ──────────────────────────────────────────────────────────
print("\n>>> Warehouse summary:")
print(f"    dim_etf        → {dim_etf.count()} ETF registered")
print(f"    fact_prices    → {fact_prices.count()} daily price records")
print(f"    fact_rolling   → {fact_rolling.count()} rolling indicator records")
print(f"    fact_anomalies → {fact_anomalies.count()} anomalous days detected")

job.commit()
print("\n>>> Job 2 completed successfully.")
