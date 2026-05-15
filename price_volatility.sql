-- models/price_volatility.sql
-- ================================================
-- PURPOSE:
--   Week-over-week price changes, % changes,
--   rolling standard deviation, price direction
--   and volatility labels.
--
-- SOURCE TABLE: USER_DB_FERRET.RAW.FUEL_PRICES
-- OUTPUT TABLE: USER_DB_FERRET.DBT.PRICE_VOLATILITY
-- ================================================

{{ config(materialized='table') }}

WITH base AS (
    SELECT
        WEEK_DATE,
        REGION,
        REGULAR_GASOLINE_PRICE,
        DIESEL_PRICE,
        LAG(REGULAR_GASOLINE_PRICE) OVER (
            PARTITION BY REGION ORDER BY WEEK_DATE
        ) AS PREV_WEEK_REGULAR,
        LAG(DIESEL_PRICE) OVER (
            PARTITION BY REGION ORDER BY WEEK_DATE
        ) AS PREV_WEEK_DIESEL
    FROM {{ source('raw', 'fuel_prices') }}
    WHERE REGULAR_GASOLINE_PRICE IS NOT NULL
      AND REGION = 'US_NATIONAL'
),

with_changes AS (
    SELECT
        WEEK_DATE,
        REGION,
        REGULAR_GASOLINE_PRICE,
        DIESEL_PRICE,

        -- Dollar change week over week
        ROUND(REGULAR_GASOLINE_PRICE - PREV_WEEK_REGULAR, 4) AS REGULAR_WOW_CHANGE,
        ROUND(DIESEL_PRICE           - PREV_WEEK_DIESEL,  4) AS DIESEL_WOW_CHANGE,

        -- Percentage change week over week
        CASE
            WHEN PREV_WEEK_REGULAR > 0
            THEN ROUND(
                ((REGULAR_GASOLINE_PRICE - PREV_WEEK_REGULAR) / PREV_WEEK_REGULAR) * 100,
                4)
            ELSE NULL
        END AS REGULAR_WOW_PCT_CHANGE,

        -- All-time historical average for this region
        AVG(REGULAR_GASOLINE_PRICE) OVER (
            PARTITION BY REGION
        ) AS HIST_AVG_REGULAR,

        -- 12-week rolling standard deviation (volatility measure)
        STDDEV(REGULAR_GASOLINE_PRICE) OVER (
            PARTITION BY REGION ORDER BY WEEK_DATE
            ROWS BETWEEN 11 PRECEDING AND CURRENT ROW
        ) AS ROLLING_12WK_STDDEV

    FROM base
)

SELECT
    WEEK_DATE,
    REGION,
    REGULAR_GASOLINE_PRICE,
    DIESEL_PRICE,
    REGULAR_WOW_CHANGE,
    DIESEL_WOW_CHANGE,
    REGULAR_WOW_PCT_CHANGE,
    ROUND(HIST_AVG_REGULAR, 4) AS HIST_AVG_REGULAR,

    -- How far above/below the historical average
    ROUND(REGULAR_GASOLINE_PRICE - HIST_AVG_REGULAR, 4) AS PRICE_ANOMALY,

    ROUND(ROLLING_12WK_STDDEV, 4) AS ROLLING_12WK_STDDEV,

    -- Price direction label
    CASE
        WHEN REGULAR_WOW_CHANGE >  0.05 THEN 'Rising'
        WHEN REGULAR_WOW_CHANGE < -0.05 THEN 'Falling'
        ELSE 'Stable'
    END AS PRICE_DIRECTION,

    -- Volatility label
    CASE
        WHEN ROLLING_12WK_STDDEV > 0.30 THEN 'High Volatility'
        WHEN ROLLING_12WK_STDDEV > 0.15 THEN 'Moderate Volatility'
        ELSE 'Low Volatility'
    END AS VOLATILITY_LABEL

FROM with_changes
ORDER BY WEEK_DATE
