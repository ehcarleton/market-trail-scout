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

recent_prices AS (
    SELECT
        s.symbol,
        s.date,
        s.close,
        ROW_NUMBER() OVER (PARTITION BY s.symbol ORDER BY s.date DESC) AS rn
    FROM eod_prices s
    JOIN symbols_with_recent_data t ON s.symbol = t.symbol
    WHERE s.date >= (SELECT date FROM latest_valid_date) - INTERVAL '10 days'
),

price_streaks AS (
    SELECT
        p0.symbol,
        p0.close AS close_today,
        p1.close AS close_yesterday,
        p2.close AS close_2_days_ago,
        CASE
            WHEN p2.close < p1.close AND p1.close < p0.close THEN 3
            WHEN p1.close < p0.close AND p2.close >= p1.close THEN 2
            WHEN p0.close > p1.close AND p1.close >= p2.close THEN 1
            ELSE 0
        END AS up_streak_score
    FROM recent_prices p0
    JOIN recent_prices p1 ON p0.symbol = p1.symbol AND p1.rn = p0.rn + 1
    JOIN recent_prices p2 ON p0.symbol = p2.symbol AND p2.rn = p0.rn + 2
    WHERE p0.rn = 1
),

highs_1yr AS (
    SELECT
        symbol,
        MAX(close) AS high_1yr
    FROM eod_prices
    WHERE date >= (SELECT MAX(date) FROM eod_prices) - INTERVAL '365 days'
    GROUP BY symbol
),

sma_data AS (
    SELECT
        symbol,
        AVG(close) FILTER (
            WHERE date >= (SELECT MAX(date) FROM eod_prices) - INTERVAL '50 days'
        ) AS sma_50,
        AVG(close) FILTER (
            WHERE date >= (SELECT MAX(date) FROM eod_prices) - INTERVAL '200 days'
        ) AS sma_200
    FROM eod_prices
    WHERE date >= (SELECT MAX(date) FROM eod_prices) - INTERVAL '200 days'
    GROUP BY symbol
)

SELECT
    ps.symbol,
    f.company_name AS security_name,
    f.industry,
    f.sector,
    f.country,
    s.exchange,
    s.market_cap,
    s.quote_type,
    s.delisted_date,

    ps.up_streak_score,
    ps.close_today,
    ps.close_yesterday,
    ps.close_2_days_ago,

    h.high_1yr,
    sd.sma_50,
    sd.sma_200
FROM price_streaks ps
JOIN highs_1yr h ON ps.symbol = h.symbol
JOIN sma_data sd ON ps.symbol = sd.symbol
JOIN symbols s ON ps.symbol = s.symbol
JOIN fundamentals f ON s.company_id = f.company_id
WHERE ps.up_streak_score > 0;
