# ETF Analytics Pipeline

Progetto sviluppato nell'ambito del Master in Artificial Intelligence & Data Analytics for Business dell'Università degli Studi di Milano-Bicocca, modulo **Big Data Processing and Data Engineering**.

## Descrizione

Sistema automatizzato su AWS per l'analisi statistica di serie storiche di ETF. Il sistema raccoglie, raccoglie, trasforma e analizza dati storici di 20 ETF su 5 categorie (Equity Sviluppati, Equity Emergenti, Obbligazionario, Commodity, Settoriale), producendo analisi statistiche accessibili via dashboard Power BI.

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
AWS Glue Job 2    → S3 /warehouse/
      │
      ▼
AWS Athena        → Query Layer (SQL + Views)
      │
      ▼
Power BI          → Dashboard (5 pagine analitiche)
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
- **Formato storage:** Parquet (più leggero del CSV)

## Domande di analisi

1. Qual è il trend di lungo periodo di ciascun ETF e categoria?
2. In quali periodi si concentra la maggiore volatilità?
3. Quali ETF sono più efficienti in termini di rapporto rendimento/rischio?
4. Esistono anomalie statistiche nei rendimenti giornalieri?
5. I rendimenti seguono una distribuzione normale?

## Stack tecnologico

| Layer | Tecnologia | Ruolo |
|-------|-----------|-------|
| Scraping | Python + yfinance | Scarica i dati storici degli ETF da Yahoo Finance |
| Schedulazione | AWS EventBridge | Avvia la pipeline automaticamente ogni giorno alle 21:00 UTC |
| Esecuzione | AWS Lambda | Esegue lo scraping senza server dedicati |
| Storage | AWS S3 | Salva i dati in tre livelli (raw, staging, warehouse) in formato Parquet |
| ETL | AWS Glue (PySpark) | Pulisce, trasforma e arricchisce i dati |
| Catalogazione | AWS Glue Catalog | Metadati e schema tabelle |
| Query | AWS Athena | Interroga i dati con SQL direttamente su S3 |
| Visualization | Power BI | Dashboard con 5 pagine di analisi interattive per categoria |

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

Progetto individuale — Master AI & Data Analytics for Business, Università degli Studi di Milano-Bicocca
