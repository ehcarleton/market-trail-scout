-- Phase 1: Identify structurally sound breakout candidates

WITH latest_valid_date AS (
    SELECT date
    FROM eod_prices
    GROUP BY date
    HAVING COUNT(*) >= 50
    ORDER BY date DESC
    LIMIT 1
),

symbols_with_recent_data AS (
    SELECT DISTINCT s.symbol
    FROM eod_prices s
    JOIN latest_valid_date l ON s.date = l.date
),

-- Precompute LAG(close) and daily move to avoid nested window functions
precalc_moves AS (
    SELECT
        s.symbol,
        s.date,
        s.close,
        s.volume,
        LAG(s.close) OVER w AS prev_close,
        ABS(s.close - LAG(s.close) OVER w) AS daily_move
    FROM eod_prices s
    JOIN symbols_with_recent_data t ON s.symbol = t.symbol
    WHERE s.date >= DATE '2025-05-02'
    WINDOW w AS (PARTITION BY s.symbol ORDER BY s.date)
),

recent_data AS (
    SELECT
        symbol,
        date,
        close,
        volume,
        AVG(close) OVER w_20 AS sma_20,
        MAX(close) OVER w_20 AS high_20d,
        MIN(close) OVER w_5 AS min_5d,
        MAX(close) OVER w_5 AS max_5d,
        AVG(daily_move) OVER w_5 AS avg_daily_move,
        AVG(volume) OVER w_20 AS avg_vol_20
    FROM precalc_moves
    WINDOW
        w_5 AS (PARTITION BY symbol ORDER BY date ROWS BETWEEN 4 PRECEDING AND CURRENT ROW),
        w_20 AS (PARTITION BY symbol ORDER BY date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW)
),

latest_snapshots AS (
    SELECT *
    FROM (
        SELECT *,
               ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY date DESC) AS rn
        FROM recent_data
    )
    WHERE rn = 1
)

SELECT
    r.symbol,
    f.company_name AS security_name,
    f.sector,
    f.industry,
    r.close AS last_close,
    r.sma_20,
    ROUND((r.close - r.high_20d) / r.high_20d, 4) AS pct_from_20d_high,
    ROUND((r.max_5d - r.min_5d) / NULLIF(r.close, 0), 4) AS pct_range_5d,
    ROUND(r.avg_daily_move / NULLIF(r.close, 0), 4) AS avg_move_pct,
    ROUND(r.volume / NULLIF(r.avg_vol_20, 0), 2) AS volume_ratio
FROM latest_snapshots r
JOIN symbols s ON r.symbol = s.symbol
JOIN fundamentals f ON s.company_id = f.company_id
WHERE r.close > r.sma_20
  AND (r.max_5d - r.min_5d) / NULLIF(r.close, 0) < 0.03
  AND (r.close - r.high_20d) / r.high_20d > -0.03
  AND r.avg_daily_move / NULLIF(r.close, 0) < 0.02
  AND r.volume / NULLIF(r.avg_vol_20, 0) BETWEEN 0.5 AND 2.5
ORDER BY pct_range_5d ASC;
