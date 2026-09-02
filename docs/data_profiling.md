# Data Profiling e Trattamenti

## Problemi identificati nei dati raw

### 1. Valori mancanti — Severità: Media

**Problema:** I mercati europei (LSE) e americani (NYSE) hanno calendari di festività diversi. Un giorno festivo in UK non è festivo negli USA e viceversa, generando gap nella serie temporale quando si analizzano ETF di entrambe le borse contemporaneamente.

**Trattamento:** Forward-fill per ticker tramite Window function PySpark:
```python
window_ffill = Window.partitionBy("ticker").orderBy("date").rowsBetween(Window.unboundedPreceding, 0)
df = df.withColumn("close", F.last(F.col("close"), ignorenulls=True).over(window_ffill))
```
Il forward-fill mantiene la continuità della serie temporale usando l'ultimo valore disponibile.

---

### 2. Duplicati — Severità: Bassa

**Problema:** Il Lambda scarica lo storico completo ad ogni run (da 2024-01-01 ad oggi). Se il job gira due volte nella stessa giornata, si creano righe duplicate per la stessa coppia `(date, ticker)`.

**Trattamento:** Deduplicazione esplicita sulla chiave naturale:
```python
df = df.dropDuplicates(["date", "ticker"])
```

---

### 3. Outlier nei volumi — Severità: Media

**Problema:** In giorni di alta volatilità di mercato (es. annunci Fed, eventi geopolitici) i volumi di scambio possono essere 5-10 volte superiori alla media storica, risultando statisticamente anomali.

**Trattamento:** Z-score detection con soglia |z| > 3. I giorni anomali vengono **flaggati** (colonna `is_volume_outlier`) ma non rimossi — preservando l'audit trail completo dei dati originali.
```python
df = df.withColumn("volume_zscore", (F.col("volume") - F.mean("volume").over(window_stats)) / F.stddev("volume").over(window_stats))
df = df.withColumn("is_volume_outlier", F.abs(F.col("volume_zscore")) > 3)
```

---

### 4. Tipi di dato inconsistenti — Severità: Media

**Problema:** yfinance 1.5.x restituisce:
- Timestamp con timezone `Europe/London` per gli ETF su LSE
- Colonne extra (`Dividends`, `Stock Splits`, `Capital Gains`) non necessarie
- Tipi numerici variabili tra versioni diverse della libreria

**Trattamento nel Lambda (prima del salvataggio):**
```python
df.index = df.index.tz_localize(None)  # rimuove timezone
df = df.drop(columns=["Dividends", "Stock Splits", "Capital Gains"], errors="ignore")
df.to_parquet(buffer, coerce_timestamps="ms")  # forza millisecondi (compatibilità Glue)
```

**Trattamento nel Job 1 (cast esplicito):**
```python
df = df.withColumn("date",   F.col("date").cast(DateType()))
df = df.withColumn("close",  F.col("close").cast(DoubleType()))
df = df.withColumn("volume", F.col("volume").cast(LongType()))
```

---

### 5. Storico disomogeneo — Severità: Bassa

**Problema:** ETF con date di lancio diverse hanno serie storiche di lunghezza diversa. Ad esempio, un ETF lanciato nel 2020 avrà meno dati di uno lanciato nel 2000, anche con lo stesso `start='2024-01-01'`.

**Trattamento:** `dropna()` dopo il forward-fill sulla colonna `close` — rimuove solo le righe irrecuperabili (quelle che precedono il lancio dell'ETF e per cui non esiste nessun valore precedente da propagare).

---

## Indicatori statistici calcolati (Feature Engineering)

| Indicatore | Formula | Job |
|-----------|---------|-----|
| Rendimento giornaliero | `(closeₜ - closeₜ₋₁) / closeₜ₋₁` | Job 1 |
| Prezzo normalizzato (base 100) | `(closeₜ / close₀) × 100` | Job 1 |
| Z-score rendimento | `(returnₜ - μ) / σ` | Job 1 |
| SMA 20 giorni | Media mobile 20 giorni | Job 2 |
| SMA 252 giorni | Media mobile ~1 anno lavorativo | Job 2 |
| Volatilità annualizzata | `std(rendimenti₃₀gg) × √252` | Job 2 |
| Drawdown | `(closeₜ - max(close₀..ₜ)) / max(close₀..ₜ)` | Job 2 |
| Return-to-risk ratio | `rendimento_cumulativo / volatilità_media` | Athena |

## Soglia anomaly detection

Un giorno è classificato come anomalo se il suo Z-score supera **2.5** in valore assoluto (approssimativamente il top/bottom 1.2% della distribuzione).

Le anomalie sono classificate per gravità:
- `moderate`: |Z| tra 2.5 e 3.0
- `high`: |Z| tra 3.0 e 4.0
- `extreme`: |Z| > 4.0

E per tipo:
- `spike_up`: rendimento anomalo positivo
- `spike_down`: rendimento anomalo negativo
