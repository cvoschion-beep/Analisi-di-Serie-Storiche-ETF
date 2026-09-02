-- ============================================================================
-- ETF Analytics Pipeline — Athena Queries v2 (20 ETF)
-- Database: etf_analytics_db
-- ============================================================================


-- ============================================================================
-- SEZIONE 1 — AGGIORNA dim_etf con le nuove categorie
-- Esegui questa prima del Crawler se dim_etf non si aggiorna automaticamente
-- ============================================================================

-- Verifica quanti ticker sono presenti
SELECT ticker, COUNT(*) as giorni
FROM etf_analytics_db.fact_prices
GROUP BY ticker
ORDER BY ticker;


-- ============================================================================
-- SEZIONE 2 — VIEWS AGGIORNATE
-- Ricrea le view esistenti + aggiungi le nuove per categoria
-- ============================================================================


-- ----------------------------------------------------------------------------
-- VIEW 1: v_prices_normalized (aggiornata — ora include 20 ETF)
-- ----------------------------------------------------------------------------
CREATE OR REPLACE VIEW etf_analytics_db.v_prices_normalized AS
SELECT
    p.date,
    p.ticker,
    e.etf_name,
    e.provider,
    e.category,
    p.close,
    p.close_norm,
    p.cumulative_return,
    p.daily_return,
    p.day_direction,
    p.volume
FROM etf_analytics_db.fact_prices p
JOIN etf_analytics_db.dim_etf e ON p.ticker = e.ticker
ORDER BY p.ticker, p.date;


-- ----------------------------------------------------------------------------
-- VIEW 2: v_category_performance
-- Rendimento medio per categoria — risponde a "quale categoria performa meglio?"
-- ----------------------------------------------------------------------------
CREATE OR REPLACE VIEW etf_analytics_db.v_category_performance AS
SELECT
    e.category,
    p.date,
    DATE_FORMAT(p.date, '%Y-%m') AS year_month,
    ROUND(AVG(p.close_norm), 2)         AS avg_close_norm,
    ROUND(AVG(p.daily_return) * 100, 4) AS avg_daily_return_pct,
    ROUND(AVG(p.cumulative_return) * 100, 2) AS avg_cumulative_return_pct,
    COUNT(DISTINCT p.ticker)             AS etf_count
FROM etf_analytics_db.fact_prices p
JOIN etf_analytics_db.dim_etf e ON p.ticker = e.ticker
GROUP BY e.category, p.date, DATE_FORMAT(p.date, '%Y-%m')
ORDER BY e.category, p.date;


-- ----------------------------------------------------------------------------
-- VIEW 3: v_category_volatility
-- Volatilità media per categoria nel tempo
-- ----------------------------------------------------------------------------
CREATE OR REPLACE VIEW etf_analytics_db.v_category_volatility AS
SELECT
    e.category,
    DATE_FORMAT(r.date, '%Y-%m') AS year_month,
    ROUND(AVG(r.volatility_30_annualized), 4) AS avg_volatility,
    ROUND(MIN(r.drawdown) * 100, 2)           AS worst_drawdown_pct,
    COUNT(DISTINCT r.ticker)                   AS etf_count
FROM etf_analytics_db.fact_rolling r
JOIN etf_analytics_db.dim_etf e ON r.ticker = e.ticker
GROUP BY e.category, DATE_FORMAT(r.date, '%Y-%m')
ORDER BY e.category, year_month;


-- ----------------------------------------------------------------------------
-- VIEW 4: v_risk_return
-- Scatter plot rischio/rendimento per ETF
-- Asse X = volatilità media, Asse Y = rendimento cumulativo
-- ----------------------------------------------------------------------------
CREATE OR REPLACE VIEW etf_analytics_db.v_risk_return AS
SELECT
    p.ticker,
    e.etf_name,
    e.category,
    e.provider,
    -- Rendimento cumulativo totale (ultimo valore disponibile)
    ROUND(MAX(p.cumulative_return) * 100, 2)          AS total_return_pct,
    -- Volatilità media annualizzata
    ROUND(AVG(r.volatility_30_annualized) * 100, 2)   AS avg_volatility_pct,
    -- Sharpe ratio semplificato (rendimento / volatilità)
    ROUND(
        MAX(p.cumulative_return) / NULLIF(AVG(r.volatility_30_annualized), 0),
    2) AS return_to_risk_ratio,
    -- Drawdown massimo
    ROUND(MIN(r.drawdown) * 100, 2)                   AS max_drawdown_pct,
    COUNT(DISTINCT p.date)                             AS trading_days
FROM etf_analytics_db.fact_prices p
JOIN etf_analytics_db.fact_rolling r
    ON p.ticker = r.ticker AND p.date = r.date
JOIN etf_analytics_db.dim_etf e ON p.ticker = e.ticker
GROUP BY p.ticker, e.etf_name, e.category, e.provider
ORDER BY total_return_pct DESC;


-- ----------------------------------------------------------------------------
-- VIEW 5: v_correlation_matrix (aggiornata — pivot su 20 ETF)
-- Nota: per 20 ETF il pivot manuale è troppo lungo
-- Usa questa view per calcolare correlazioni a coppie in Power BI
-- ----------------------------------------------------------------------------
CREATE OR REPLACE VIEW etf_analytics_db.v_returns_daily AS
SELECT
    date,
    ticker,
    daily_return,
    close_norm,
    cumulative_return
FROM etf_analytics_db.fact_prices
WHERE daily_return IS NOT NULL
ORDER BY date, ticker;


-- ----------------------------------------------------------------------------
-- VIEW 6: v_volatility_heatmap (aggiornata)
-- ----------------------------------------------------------------------------
CREATE OR REPLACE VIEW etf_analytics_db.v_volatility_heatmap AS
SELECT
    r.ticker,
    e.etf_name,
    e.category,
    DATE_FORMAT(r.date, '%Y-%m') AS year_month,
    YEAR(r.date)                 AS year,
    MONTH(r.date)                AS month,
    ROUND(AVG(r.volatility_30_annualized), 4) AS avg_volatility,
    ROUND(AVG(p.daily_return), 4)             AS avg_daily_return,
    ROUND((MAX(p.close_norm) - MIN(p.close_norm)) / MIN(p.close_norm), 4) AS monthly_range
FROM etf_analytics_db.fact_rolling r
JOIN etf_analytics_db.fact_prices p ON r.ticker = p.ticker AND r.date = p.date
JOIN etf_analytics_db.dim_etf e ON r.ticker = e.ticker
GROUP BY r.ticker, e.etf_name, e.category,
         DATE_FORMAT(r.date, '%Y-%m'), YEAR(r.date), MONTH(r.date)
ORDER BY r.ticker, year_month;


-- ----------------------------------------------------------------------------
-- VIEW 7: v_drawdown_analysis (aggiornata)
-- ----------------------------------------------------------------------------
CREATE OR REPLACE VIEW etf_analytics_db.v_drawdown_analysis AS
SELECT
    r.date,
    r.ticker,
    e.etf_name,
    e.category,
    ROUND(r.drawdown * 100, 2)           AS drawdown_pct,
    ROUND(r.volatility_30_annualized, 4) AS volatility_ann,
    r.sma_20,
    r.sma_252,
    CASE
        WHEN p.close > r.sma_252 THEN 'above_sma252'
        WHEN p.close < r.sma_252 THEN 'below_sma252'
        ELSE 'at_sma252'
    END AS trend_signal
FROM etf_analytics_db.fact_rolling r
JOIN etf_analytics_db.fact_prices p ON r.ticker = p.ticker AND r.date = p.date
JOIN etf_analytics_db.dim_etf e ON r.ticker = e.ticker
ORDER BY r.ticker, r.date;


-- ----------------------------------------------------------------------------
-- VIEW 8: v_anomalies_detail (aggiornata)
-- ----------------------------------------------------------------------------
CREATE OR REPLACE VIEW etf_analytics_db.v_anomalies_detail AS
SELECT
    a.date,
    a.ticker,
    e.etf_name,
    e.provider,
    e.category,
    ROUND(a.daily_return * 100, 2) AS daily_return_pct,
    ROUND(a.return_zscore, 2)      AS return_zscore,
    a.anomaly_type,
    a.anomaly_severity,
    a.volume,
    ROUND(a.volume_zscore, 2)      AS volume_zscore,
    CASE WHEN ABS(a.volume_zscore) > 2 THEN true ELSE false END AS high_volume_anomaly
FROM etf_analytics_db.fact_anomalies a
JOIN etf_analytics_db.dim_etf e ON a.ticker = e.ticker
ORDER BY a.date DESC, ABS(a.return_zscore) DESC;


-- ============================================================================
-- SEZIONE 3 — QUERY DI ANALISI FINALE (esporta come CSV per Power BI)
-- ============================================================================


-- Query 1: Verifica dati — righe per ticker
SELECT ticker, COUNT(*) as giorni,
       MIN(date) as prima_data, MAX(date) as ultima_data
FROM etf_analytics_db.fact_prices
GROUP BY ticker
ORDER BY ticker;


-- Query 2: Risk/Return per categoria (per scatter plot)
SELECT * FROM etf_analytics_db.v_risk_return
ORDER BY category, total_return_pct DESC;


-- Query 3: Performance media per categoria nel tempo
SELECT * FROM etf_analytics_db.v_category_performance
ORDER BY category, date;


-- Query 4: Volatilità per categoria nel tempo
SELECT * FROM etf_analytics_db.v_category_volatility
ORDER BY category, year_month;


-- Query 5: Top 5 ETF per rendimento cumulativo
SELECT ticker, etf_name, category, total_return_pct, avg_volatility_pct,
       return_to_risk_ratio, max_drawdown_pct
FROM etf_analytics_db.v_risk_return
ORDER BY total_return_pct DESC
LIMIT 5;


-- Query 6: Top 5 ETF per rapporto rendimento/rischio
SELECT ticker, etf_name, category, total_return_pct, avg_volatility_pct,
       return_to_risk_ratio, max_drawdown_pct
FROM etf_analytics_db.v_risk_return
ORDER BY return_to_risk_ratio DESC
LIMIT 5;


-- Query 7: Anomalie per categoria
SELECT e.category, COUNT(*) as anomalie_totali,
       SUM(CASE WHEN a.anomaly_type = 'spike_up' THEN 1 ELSE 0 END) as spike_up,
       SUM(CASE WHEN a.anomaly_type = 'spike_down' THEN 1 ELSE 0 END) as spike_down,
       SUM(CASE WHEN a.anomaly_severity = 'extreme' THEN 1 ELSE 0 END) as extreme
FROM etf_analytics_db.fact_anomalies a
JOIN etf_analytics_db.dim_etf e ON a.ticker = e.ticker
GROUP BY e.category
ORDER BY anomalie_totali DESC;


-- Query 8: Rendimenti giornalieri (per istogramma distribuzione)
SELECT ticker, category,
       ROUND(daily_return * 100 / 0.5) * 0.5 AS return_bucket,
       COUNT(*) AS frequency
FROM etf_analytics_db.v_prices_normalized
WHERE daily_return IS NOT NULL
GROUP BY ticker, category, ROUND(daily_return * 100 / 0.5) * 0.5
ORDER BY ticker, return_bucket;
