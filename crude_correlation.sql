-- models/crude_correlation.sql
-- ================================================
-- PURPOSE:
--   Joins retail gasoline prices (EIA) with
--   crude oil futures prices (yfinance CL=F).
--   Shows how crude oil prices feed through to
--   the pump with a 2-week lag.
--
-- SOURCE TABLES:
--   USER_DB_FERRET.RAW.FUEL_PRICES           (EIA)
--   USER_DB_FERRET.RAW.ENERGY_MARKET_PRICES  (yfinance)
-- OUTPUT TABLE:
--   USER_DB_FERRET.DBT.CRUDE_CORRELATION
-- ================================================

{{ config(materialized='table') }}

WITH fuel AS (
    SELECT
        WEEK_DATE,
        REGULAR_GASOLINE_PRICE,
        DIESEL_PRICE
    FROM {{ source('raw', 'fuel_prices') }}
    WHERE REGION = 'US_NATIONAL'
      AND REGULAR_GASOLINE_PRICE IS NOT NULL
),

-- Pull only WTI Crude Oil (CL=F) from energy market table
crude AS (
    SELECT
        WEEK_DATE,
        CLOSE_PRICE                          AS CRUDE_CLOSE_USD_BBL,
        -- Convert $/barrel to $/gallon (1 barrel = 42 gallons)
        ROUND(CLOSE_PRICE / 42.0, 4)         AS CRUDE_USD_PER_GALLON
    FROM {{ source('raw', 'energy_market_prices') }}
    WHERE TICKER = 'CL=F'
      AND CLOSE_PRICE IS NOT NULL
),

-- Add a 2-week lag on crude price (typical refinery processing time)
crude_lagged AS (
    SELECT
        WEEK_DATE,
        CRUDE_CLOSE_USD_BBL,
        CRUDE_USD_PER_GALLON,
        LAG(CRUDE_CLOSE_USD_BBL,   2) OVER (ORDER BY WEEK_DATE) AS CRUDE_LAG2_BBL,
        LAG(CRUDE_USD_PER_GALLON,  2) OVER (ORDER BY WEEK_DATE) AS CRUDE_LAG2_GAL
    FROM crude
)

SELECT
    f.WEEK_DATE,
    f.REGULAR_GASOLINE_PRICE,
    f.DIESEL_PRICE,
    c.CRUDE_CLOSE_USD_BBL,
    c.CRUDE_USD_PER_GALLON,
    c.CRUDE_LAG2_BBL,
    c.CRUDE_LAG2_GAL,

    -- Pump-to-crude margin (retail price minus crude cost per gallon)
    ROUND(
        f.REGULAR_GASOLINE_PRICE
        - COALESCE(c.CRUDE_LAG2_GAL, c.CRUDE_USD_PER_GALLON),
        4
    ) AS PUMP_TO_CRUDE_MARGIN,

    -- Margin category
    CASE
        WHEN ROUND(f.REGULAR_GASOLINE_PRICE
             - COALESCE(c.CRUDE_LAG2_GAL, c.CRUDE_USD_PER_GALLON), 4) > 1.50
        THEN 'Wide Margin'
        WHEN ROUND(f.REGULAR_GASOLINE_PRICE
             - COALESCE(c.CRUDE_LAG2_GAL, c.CRUDE_USD_PER_GALLON), 4) > 0.80
        THEN 'Normal Margin'
        ELSE 'Tight Margin'
    END AS MARGIN_CATEGORY

FROM fuel f
LEFT JOIN crude_lagged c ON f.WEEK_DATE = c.WEEK_DATE
ORDER BY f.WEEK_DATE
