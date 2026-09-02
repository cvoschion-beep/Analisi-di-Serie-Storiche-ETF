# ETF Analytics Pipeline

Progetto sviluppato nell'ambito del Master in Business Intelligence & Big Data Analytics dell'Università degli Studi di Milano-Bicocca, modulo **Big Data Analytics — Modern Architectures**.

## Descrizione

Pipeline Big Data end-to-end su AWS per l'analisi statistica di serie storiche di ETF. Il sistema raccoglie automaticamente ogni giorno i dati di 20 ETF su 5 categorie diverse (Equity Sviluppati, Equity Emergenti, Obbligazionario, Commodity, Settoriale), li trasforma garantendo qualità dei dati, e produce analisi statistiche visualizzate tramite dashboard Power BI.

L'obiettivo non è l'analisi finanziaria tradizionale, ma l'applicazione di strumenti statistici (Z-score, volatilità rolling, normalizzazione, anomaly detection) su serie temporali finanziarie — senza richiedere competenze di dominio finanziario.

## Architettura

```
EventBridge (cron 21:00 UTC)
      │
      ▼
AWS Lambda (scraping via yfinance)
      │
      ▼
S3 /raw/          → Source Tables (Parquet)
      │
      ▼
AWS Glue Job 1    → S3 /staging/ (Data Quality)
      │
      ▼
AWS Glue Job 2    → S3 /warehouse/ (Feature Engineering)
      │
      ▼
AWS Athena        → Query Layer (SQL + Views)
      │
      ▼
Power BI          → Dashboard (4 pagine analitiche)
```

## Dataset

| Categoria | ETF |
|-----------|-----|
| Equity Sviluppati | SWRD.L · SPY · QQQ · EFA · VEUR.L · CSPX.L |
| Equity Emergenti | EIMI.L · VFEM.L |
| Obbligazionario | TLT · AGG · HYG · LQD |
| Commodity | GLD · SLV · USO · PDBC |
| Settoriale | XLK · XLV · XLE · ARKK |

- **Fonte:** Yahoo Finance via `yfinance`
- **Periodo:** 2024-01-02 → oggi
- **Cardinalità:** ~13.300 righe totali (~665 giorni × 20 ETF)
- **Formato storage:** Parquet (compressione ~5× vs CSV)

## Domande di analisi

1. Qual è il trend di lungo periodo di ciascun ETF e categoria?
2. In quali periodi si concentra la maggiore volatilità?
3. Quali ETF sono più efficienti in termini di rapporto rendimento/rischio?
4. Esistono anomalie statistiche nei rendimenti giornalieri?
5. I rendimenti seguono una distribuzione normale?

## Stack tecnologico

| Layer | Tecnologia | Ruolo |
|-------|-----------|-------|
| Scraping | Python + yfinance | Estrazione dati da Yahoo Finance |
| Orchestrazione | AWS EventBridge | Trigger giornaliero automatico |
| Compute | AWS Lambda | Esecuzione scraping serverless |
| Storage | AWS S3 | Data Lake con 3 zone separate |
| ETL | AWS Glue (PySpark) | Trasformazione e feature engineering |
| Catalogazione | AWS Glue Catalog | Metadati e schema tabelle |
| Query | AWS Athena | SQL serverless su S3 |
| Visualization | Power BI | Dashboard interattiva |

## Struttura del repository

```
etf-analytics-pipeline/
├── README.md
├── lambda/
│   └── lambda_ingestion.py         # Scraping giornaliero via AWS Lambda
├── glue/
│   ├── glue_job1_raw_to_staging.py # ETL: Raw → Staging (Data Quality)
│   └── glue_job2_staging_to_warehouse.py # ETL: Staging → Warehouse
├── athena/
│   └── athena_queries.sql          # View SQL e query di analisi
└── docs/
    ├── architettura.md             # Descrizione dettagliata architettura
    └── data_profiling.md           # Problemi identificati e trattamenti
```

## Note tecniche

- **yfinance 1.5.x:** usare `actions=False` nel metodo `.history()` per evitare il bug di storico tronco
- **Timestamp:** applicare `tz_localize(None)` prima del salvataggio Parquet per compatibilità con AWS Glue
- **Layer Lambda:** installare le dipendenze con `--only-binary=:all:` e rimuovere `pygments`, `rich`, `curl_cffi` per rispettare il limite di 250MB
- **S3 path:** usare path piatto `raw/ticker=GLD/data.parquet` (no partizione per data) per evitare conflitti di schema in Glue

## Requisiti

- Account AWS (testato su AWS Academy con budget $50)
- Python 3.12
- Dipendenze Lambda: `yfinance`, `pandas`, `pyarrow`, `boto3`
- Power BI (online o desktop)

## Autore

Progetto individuale — Master BI & Big Data Analytics, Università degli Studi di Milano-Bicocca
