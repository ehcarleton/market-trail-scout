-- 1. Get symbols with valid recent data
WITH symbols_with_latest AS (
    SELECT symbol
    FROM eod_prices
    WHERE date = (
        SELECT date
        FROM eod_prices
        GROUP BY date
        HAVING COUNT(*) >= 50
        ORDER BY date DESC
        LIMIT 1
    )
),

-- 2. Base price data for swing analysis
base_data AS (
    SELECT s.symbol, s.date, s.close, s.volume,
           CAST(epoch(s.date) AS INT) AS ordinal_date
    FROM eod_prices s
    JOIN symbols_with_latest t ON s.symbol = t.symbol
    WHERE s.date >= (SELECT MAX(date) FROM eod_prices) - INTERVAL '70 days'
),

-- 3. Score swing pivots
scored_pivots AS (
    SELECT *,
        CASE
            WHEN LAG(close,4) OVER w < LAG(close,3) OVER w AND
                 LAG(close,3) OVER w < LAG(close,2) OVER w AND
                 LAG(close,2) OVER w < LAG(close,1) OVER w AND
                 LAG(close,1) OVER w < close THEN 4
            WHEN LAG(close,3) OVER w < LAG(close,2) OVER w AND
                 LAG(close,2) OVER w < LAG(close,1) OVER w AND
                 LAG(close,1) OVER w < close THEN 3
            WHEN LAG(close,2) OVER w < LAG(close,1) OVER w AND
                 LAG(close,1) OVER w < close THEN 2
            WHEN LAG(close,1) OVER w < close THEN 1
            ELSE 0
        END AS high_strength,
        CASE
            WHEN LAG(close,4) OVER w > LAG(close,3) OVER w AND
                 LAG(close,3) OVER w > LAG(close,2) OVER w AND
                 LAG(close,2) OVER w > LAG(close,1) OVER w AND
                 LAG(close,1) OVER w > close THEN 4
            WHEN LAG(close,3) OVER w > LAG(close,2) OVER w AND
                 LAG(close,2) OVER w > LAG(close,1) OVER w AND
                 LAG(close,1) OVER w > close THEN 3
            WHEN LAG(close,2) OVER w > LAG(close,1) OVER w AND
                 LAG(close,1) OVER w > close THEN 2
            WHEN LAG(close,1) OVER w > close THEN 1
            ELSE 0
        END AS low_strength
    FROM base_data
    WINDOW w AS (PARTITION BY symbol ORDER BY date)
),

-- 4. Tag swing types
swing_points AS (
    SELECT *,
           CASE
               WHEN high_strength >= 2 THEN 1
               WHEN low_strength >= 2 THEN -1
               ELSE NULL
           END AS swing_type
    FROM scored_pivots
),

-- 5. Calculate trendlines
trendlines AS (
    SELECT
        symbol,
        COUNT(*) FILTER (WHERE swing_type = 1) AS pivot_high_count,
        AVG(high_strength) FILTER (WHERE swing_type = 1) AS pivot_high_strength_avg,
        REGR_SLOPE(close, ordinal_date) FILTER (WHERE swing_type = 1) AS resistance_slope,
        REGR_INTERCEPT(close, ordinal_date) FILTER (WHERE swing_type = 1) AS resistance_intercept,
        REGR_R2(close, ordinal_date) FILTER (WHERE swing_type = 1) AS resistance_r2,

        COUNT(*) FILTER (WHERE swing_type = -1) AS pivot_low_count,
        AVG(low_strength) FILTER (WHERE swing_type = -1) AS pivot_low_strength_avg,
        REGR_SLOPE(close, ordinal_date) FILTER (WHERE swing_type = -1) AS support_slope,
        REGR_INTERCEPT(close, ordinal_date) FILTER (WHERE swing_type = -1) AS support_intercept,
        REGR_R2(close, ordinal_date) FILTER (WHERE swing_type = -1) AS support_r2,

        MIN(date) AS start_date,
        MAX(date) AS end_date
    FROM swing_points
    WHERE swing_type IS NOT NULL
    GROUP BY symbol
),

-- 6. SMA, close, volume breakout
trend_filter AS (
    SELECT
        symbol,
        AVG(close) FILTER (
            WHERE date >= (SELECT MAX(date) FROM eod_prices) - INTERVAL '50 days'
        ) AS sma_50,
        FIRST(close ORDER BY date DESC) AS last_close,
        AVG(volume) FILTER (
            WHERE date >= (SELECT MAX(date) FROM eod_prices) - INTERVAL '50 days'
        ) AS avg_volume_50,
        FIRST(volume ORDER BY date DESC) AS last_volume
    FROM base_data
    GROUP BY symbol
)

-- 7. Final output with metadata
SELECT
    t.symbol,
    f.company_name AS security_name,
    f.industry,
    f.sector,
    f.country,
    s.exchange,
    s.market_cap,
    s.quote_type,
    s.delisted_date,

    tf.last_close,
    tf.sma_50,
    tf.avg_volume_50,
    tf.last_volume,
    ROUND(tf.last_volume / NULLIF(tf.avg_volume_50, 0), 2) AS volume_ratio,

    t.pivot_high_count,
    t.pivot_high_strength_avg,
    t.resistance_slope,
    t.resistance_intercept,
    t.resistance_r2,

    t.pivot_low_count,
    t.pivot_low_strength_avg,
    t.support_slope,
    t.support_intercept,
    t.support_r2,

    ROUND(
        COALESCE(t.support_slope, 0) * COALESCE(t.support_r2, 0) * t.pivot_low_count -
        COALESCE(t.resistance_slope, 0) * COALESCE(t.resistance_r2, 0) * t.pivot_high_count,
        6
    ) AS bullish_score,

    t.start_date,
    t.end_date

FROM trendlines t
JOIN trend_filter tf ON t.symbol = tf.symbol
JOIN symbols s ON t.symbol = s.symbol
JOIN fundamentals f ON s.company_id = f.company_id
WHERE tf.last_close > tf.sma_50
  AND (resistance_slope IS NOT NULL OR support_slope IS NOT NULL);
