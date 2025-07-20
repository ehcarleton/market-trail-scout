WITH trades_grouped AS (
    SELECT
        symbol,
        account_number,
        SUM(CASE WHEN action = 'buy' THEN quantity ELSE 0 END) AS total_bought,
        SUM(CASE WHEN action = 'sell' THEN quantity ELSE 0 END) AS total_sold,
        SUM(CASE WHEN action = 'buy' THEN quantity * price ELSE 0 END) AS total_cost,
        SUM(CASE WHEN action = 'sell' THEN quantity * price ELSE 0 END) AS total_proceeds,
        MIN(CASE WHEN action = 'buy' THEN trade_date END) AS first_buy_date,
        MAX(CASE WHEN action = 'sell' THEN trade_date END) AS last_sell_date
    FROM trades
    GROUP BY symbol, account_number
),
closed_trades AS (
    SELECT *
    FROM trades_grouped
    WHERE ABS(total_bought - total_sold) < 1.999
),
detailed_report AS (
    SELECT
        symbol,
        account_number,
        total_bought,
        total_sold,
        total_bought - total_sold AS shares_remaining,
        ROUND(total_cost, 2) AS total_cost,
        ROUND(total_proceeds, 2) AS total_proceeds,
        ROUND(total_proceeds - total_cost, 2) AS net_gain,
        ROUND((total_proceeds - total_cost) / NULLIF(total_cost, 0) * 100, 2) AS pct_gain,
        first_buy_date,
        last_sell_date,
        CAST(last_sell_date AS DATE) - CAST(first_buy_date AS DATE) AS holding_days
    FROM closed_trades
    where total_cost > 0 and total_proceeds > 0 and shares_remaining >= 0
),
summary_row AS (
    SELECT * FROM (
        SELECT
            'TOTAL' AS symbol,
            NULL AS account_number,
            NULL AS total_bought,
            NULL AS total_sold,
            NULL AS shares_remaining,
            NULL AS total_cost,
            NULL AS total_proceeds,
            ROUND(SUM(CASE WHEN net_gain > 0 THEN net_gain ELSE 0 END), 2) AS net_gain,
            ROUND(SUM(CASE WHEN net_gain > 0 THEN net_gain ELSE 0 END) / NULLIF(SUM(total_cost), 0) * 100, 2) AS pct_gain,
            NULL AS first_buy_date,
            NULL AS last_sell_date,
            NULL AS holding_days
        FROM detailed_report

        UNION ALL

        SELECT
            'TOTAL_LOSS', NULL, NULL, NULL, NULL, NULL, NULL,
            ROUND(SUM(CASE WHEN net_gain < 0 THEN net_gain ELSE 0 END), 2),
            ROUND(SUM(CASE WHEN net_gain < 0 THEN net_gain ELSE 0 END) / NULLIF(SUM(total_cost), 0) * 100, 2),
            NULL, NULL, NULL
        FROM detailed_report

        UNION ALL

        SELECT
            'NET_TOTAL', NULL, NULL, NULL, NULL, NULL, NULL,
            ROUND(SUM(net_gain), 2),
            ROUND(SUM(net_gain) / NULLIF(SUM(total_cost), 0) * 100, 2),
            NULL, NULL, NULL
        FROM detailed_report
    )
),
combined AS (
    SELECT * FROM detailed_report
    UNION ALL
    SELECT * FROM summary_row
)
SELECT *
FROM combined
ORDER BY
    CASE symbol
        WHEN 'TOTAL' THEN 1
        WHEN 'TOTAL_LOSS' THEN 2
        WHEN 'NET_TOTAL' THEN 3
        ELSE 0
    END,
    account_number,
    last_sell_date DESC,
    symbol;
