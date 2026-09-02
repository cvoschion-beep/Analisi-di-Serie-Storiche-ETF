# Architettura del Sistema

## Panoramica

Il sistema segue il pattern **Lambda Architecture** su AWS, con un Data Lake strutturato in tre zone separate su S3. L'intera pipeline è serverless — non ci sono server da gestire e il costo stimato è inferiore a 5$/mese per i volumi attuali.

## Flusso dei dati

### 1. Ingestion Layer — AWS Lambda

Il Lambda viene triggerato ogni giorno alle 21:00 UTC da **AWS EventBridge** (cron schedulato), dopo la chiusura dei mercati europei (LSE).

Per ogni ticker nel dizionario `TICKERS`:
- Chiama `yfinance.Ticker.history()` con `start='2024-01-01'` e `actions=False`
- Rimuove il timezone dal timestamp (`tz_localize(None)`)
- Serializza il DataFrame in formato Parquet con `coerce_timestamps='ms'`
- Salva su S3 nel path: `raw/ticker=EIMI.L/data.parquet`

**Nota:** ogni run sovrascrive il file con lo storico completo aggiornato. La scelta di un path piatto (senza partizione per data) evita conflitti tra la struttura delle cartelle S3 e lo schema interno del file Parquet.

### 2. Storage Layer — AWS S3

Il Data Lake è strutturato in tre zone:

| Zona | Path S3 | Contenuto | Immutabilità |
|------|---------|-----------|-------------|
| Source Tables | `/raw/` | Dati grezzi da yfinance | Sovrascrittura giornaliera |
| Staging Area | `/staging/` | Dati puliti + indicatori base | Sovrascrittura ad ogni ETL |
| Data Warehouse | `/warehouse/` | Tabelle finali per analisi | Sovrascrittura ad ogni ETL |

Formato storage: **Parquet** — formato colonnare che permette ad Athena di leggere solo le colonne necessarie per ogni query, riducendo costi e tempi.

### 3. ETL Layer — AWS Glue (PySpark)

Due job PySpark eseguiti in sequenza tramite **AWS Glue Workflow**:

#### Job 1: Raw → Staging (Data Quality)

1. Lettura dei file Parquet da `/raw/` con opzione `basePath` per evitare conflitti di schema
2. Cast esplicito dei tipi di dato (DateType, DoubleType, LongType)
3. Deduplicazione su chiave `(date, ticker)`
4. Forward-fill dei valori mancanti (festività diverse LSE vs NYSE)
5. Z-score detection degli outlier sui volumi (`is_volume_outlier`)
6. Calcolo del rendimento giornaliero percentuale
7. Normalizzazione prezzi base 100 (`close_norm`)
8. Calcolo Z-score dei rendimenti (per anomaly detection nel Job 2)

#### Job 2: Staging → Warehouse (Feature Engineering)

Costruisce lo Star Schema con 4 tabelle:

- **`dim_etf`** — anagrafica dei 20 ETF con categoria, provider, borsa
- **`fact_prices`** — serie storica giornaliera con indicatori base
- **`fact_rolling`** — indicatori su finestre mobili (SMA 20/252, volatilità 30gg, drawdown)
- **`fact_anomalies`** — giorni con Z-score > 2.5 (anomalie statistiche)

### 4. Query Layer — AWS Athena

Athena interroga direttamente i file Parquet su S3 tramite il **Glue Data Catalog** (aggiornato dal Crawler dopo ogni run del Job 2).

Sono definite 8 view SQL che alimentano Power BI:

| View | Risponde a |
|------|-----------|
| `v_prices_normalized` | Trend generale tutti gli ETF |
| `v_category_performance` | Performance media per categoria |
| `v_category_volatility` | Volatilità per categoria nel tempo |
| `v_risk_return` | Scatter rendimento/rischio per ETF |
| `v_volatility_heatmap` | Heatmap volatilità mensile |
| `v_drawdown_analysis` | Drawdown nel tempo |
| `v_anomalies_detail` | Giorni anomali con contesto |
| `v_returns_daily` | Rendimenti giornalieri per distribuzione |

### 5. Visualization Layer — Power BI

Dashboard con 4 pagine analitiche, alimentata da CSV esportati da Athena:

- **Overview** — Trend 20 ETF normalizzati base 100 + slicer categoria
- **Analisi per Categoria** — Performance e volatilità aggregate per categoria
- **Risk vs Return** — Scatter plot efficienza ETF (volatilità vs rendimento)
- **Anomaly Detection** — Distribuzione e dettaglio dei giorni anomali

## Orchestrazione

Il **Glue Workflow** (`etf-daily-pipeline`) coordina la sequenza:

```
EventBridge (21:00 UTC)
      │ trigger
      ▼
Lambda (scraping)
      │ on: Succeeded
      ▼
Glue Job 1 (raw → staging)
      │ on: Succeeded
      ▼
Glue Job 2 (staging → warehouse)
      │ on: Succeeded
      ▼
Glue Crawler (aggiorna catalogo Athena)
```

**Nota su AWS Academy:** l'ambiente accademico non mantiene i trigger schedulati attivi quando la sessione Lab è chiusa. In produzione si utilizzerebbe un account AWS dedicato o un orchestratore managed (Airflow, Prefect).

## Schema del Data Warehouse

```
              dim_etf (PK: ticker)
                    │
        ┌───────────┼───────────┐
        │           │           │
  fact_prices  fact_rolling  fact_anomalies
  (date, ticker) (date, ticker) (date, ticker)
```

Modello a stella (Star Schema): `dim_etf` è la dimensione, le tabelle `fact_*` sono i fatti.
