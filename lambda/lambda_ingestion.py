"""
ETF Analytics Pipeline — Lambda Ingestion Layer
------------------------------------------------
Triggered daily by EventBridge.
Downloads ETF data via yfinance and saves raw Parquet files to S3.
"""

import json
import logging
import io
from datetime import date, timedelta

import boto3
import pandas as pd
import yfinance as yf

# ── Logging ──────────────────────────────────────────────────────────────────
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# ── Config ────────────────────────────────────────────────────────────────────
S3_BUCKET = "etf-analytics-pipeline-voschion"   # <-- cambia col tuo bucket name
RAW_PREFIX = "raw"

# ETF principali: Mercati Emergenti vs Mercati Sviluppati + 2 di confronto
TICKERS = {
    "SWRD.L": "SPDR MSCI World",
    "SPY":    "SPDR S&P 500",
    "QQQ":    "Invesco Nasdaq 100",
    "EFA":    "iShares MSCI EAFE",
    "VEUR.L": "Vanguard FTSE Developed Europe",
    "EIMI.L": "iShares Core MSCI EM IMI",
    "CSPX.L": "iShares Core S&P 500 UCITS",
    "VFEM.L": "Vanguard FTSE Emerging Markets",
    "TLT":    "iShares 20+ Year Treasury Bond",
    "AGG":    "iShares Core US Aggregate Bond",
    "HYG":    "iShares iBoxx High Yield",
    "LQD":    "iShares Investment Grade Corp",
    "GLD":    "SPDR Gold Shares",
    "SLV":    "iShares Silver Trust",
    "USO":    "United States Oil Fund",
    "PDBC":   "Invesco Commodity Basket",
    "XLK":    "Technology Select Sector SPDR",
    "XLV":    "Health Care Select Sector SPDR",
    "XLE":    "Energy Select Sector SPDR",
    "ARKK":   "ARK Innovation ETF",
}


# ── Helpers ───────────────────────────────────────────────────────────────────
def download_etf(ticker: str, start: str, end: str) -> pd.DataFrame:
    logger.info(f"Downloading {ticker} from {start} to {end}")
    
    t = yf.Ticker(ticker)
    df = t.history(
        start="2024-01-01",
        end=date.today().isoformat(),
        interval="1d",
        auto_adjust=True,
        actions=False
    )
    
    if df.empty:
        logger.warning(f"No data returned for {ticker}")
        return pd.DataFrame()

    df.index = df.index.tz_localize(None)
    df = df.reset_index()
    df.columns = [c.lower().replace(" ", "_") for c in df.columns]
    df["ticker"] = ticker
    df["etf_name"] = TICKERS[ticker]
    df["ingestion_date"] = date.today().isoformat()

    logger.info(f"Downloaded {len(df)} rows for {ticker}")
    return df

def save_to_s3(df: pd.DataFrame, ticker: str, run_date: str) -> str:
    """
    Salva un DataFrame come Parquet su S3.
    Path: raw/ticker=IWRD.L/date=2025-06-01/data.parquet
    Struttura partizionata → ottimizza le query Athena.
    """
    s3_client = boto3.client("s3")

    path = f"{RAW_PREFIX}/ticker={ticker}/data.parquet"

    # Serializza in memoria (Lambda non ha disco scrivibile tranne /tmp)
    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False, engine="pyarrow",
              coerce_timestamps="ms",
              allow_truncated_timestamps=True)
    buffer.seek(0)

    s3_client.put_object(
        Bucket=S3_BUCKET,
        Key=path,
        Body=buffer.getvalue(),
        ContentType="application/octet-stream",
    )

    logger.info(f"Saved s3://{S3_BUCKET}/{path} ({len(df)} rows)")
    return path


# ── Handler principale ────────────────────────────────────────────────────────
def lambda_handler(event, context):
    """
    Entry point Lambda.
    Scarica gli ultimi 5 giorni per ogni ticker (copre weekend e festività)
    e salva solo i dati nuovi su S3.
    """
    today = date.today()
    run_date = today.isoformat()

    # Scarica 5 giorni indietro per essere sicuri di non perdere dati
    # (mercati chiusi nei weekend → yfinance restituisce solo giorni di trading)
    start = (today - timedelta(days=730)).isoformat()
    end = run_date

    results = {
        "run_date": run_date,
        "tickers_processed": [],
        "tickers_failed": [],
    }

    for ticker in TICKERS:
        try:
            df = download_etf(ticker, start, end)

            if df.empty:
                results["tickers_failed"].append(ticker)
                continue

            save_to_s3(df, ticker, run_date)
            results["tickers_processed"].append(ticker)

        except Exception as e:
            logger.error(f"Error processing {ticker}: {str(e)}")
            results["tickers_failed"].append(ticker)

    logger.info(f"Run summary: {results}")

    # Ritorna 200 solo se almeno un ticker è andato a buon fine
    status_code = 200 if results["tickers_processed"] else 500

    return {
        "statusCode": status_code,
        "body": json.dumps(results),
    } 
