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
S3_BUCKET = "etf-analytics-pipeline-2025"   # <-- cambia col tuo bucket name
RAW_PREFIX = "raw"

# ETF principali: Mercati Emergenti vs Mercati Sviluppati + 2 di confronto
TICKERS = {
    "EIMI.L": "iShares Core MSCI EM IMI",        # mercati emergenti
    "SWRD.L": "SPDR MSCI World",                 # mercati sviluppati
    "TLT":    "iShares 20+ Year Treasury Bond",  # confronto obbligazionario
    "GLD":    "SPDR Gold Shares",                # confronto commodity
}


# ── Helpers ───────────────────────────────────────────────────────────────────
def download_etf(ticker: str, start: str, end: str) -> pd.DataFrame:
    """
    Scarica i dati OHLCV per un singolo ticker.
    Restituisce un DataFrame con colonna 'ticker' aggiunta.
    """
    logger.info(f"Downloading {ticker} from {start} to {end}")
    df = yf.download(
        ticker,
        start=start,
        end=end,
        interval="1d",
        auto_adjust=True,   # aggiusta prezzi per dividendi/split
        progress=False,
    )

    if df.empty:
        logger.warning(f"No data returned for {ticker}")
        return pd.DataFrame()

    # Appiattisce MultiIndex se presente (yfinance >= 0.2 lo genera)
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)

    df = df.reset_index()
    df.columns = [c.lower().replace(" ", "_") for c in df.columns]

    # Aggiunge metadati utili per il downstream
    df["ticker"] = ticker
    df["etf_name"] = TICKERS[ticker]
    df["ingestion_date"] = date.today().isoformat()

    return df


def save_to_s3(df: pd.DataFrame, ticker: str, run_date: str) -> str:
    """
    Salva un DataFrame come Parquet su S3.
    Path: raw/ticker=IWRD.L/date=2025-06-01/data.parquet
    Struttura partizionata → ottimizza le query Athena.
    """
    s3_client = boto3.client("s3")

    path = f"{RAW_PREFIX}/ticker={ticker}/date={run_date}/data.parquet"

    # Serializza in memoria (Lambda non ha disco scrivibile tranne /tmp)
    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False, engine="pyarrow")
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
    start = (today - timedelta(days=5)).isoformat()
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
