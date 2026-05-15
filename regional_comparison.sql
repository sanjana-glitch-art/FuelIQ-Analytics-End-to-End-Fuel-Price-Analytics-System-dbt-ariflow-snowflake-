-- models/regional_comparison.sql
-- ================================================
-- PURPOSE:
--   Compares each PADD region's gasoline price
--   against the U.S. national average.
--   Shows which regions are cheapest/most expensive.
--
-- SOURCE TABLES:
--   USER_DB_FERRET.RAW.FUEL_PRICES           (national)
--   USER_DB_FERRET.RAW.REGIONAL_FUEL_PRICES  (regional)
-- OUTPUT TABLE:
--   USER_DB_FERRET.DBT.REGIONAL_COMPARISON
-- ================================================

{{ config(materialized='table') }}

WITH regional AS (
    SELECT
        WEEK_DATE,
        REGION,
        PRICE AS REGIONAL_PRICE
    FROM {{ source('raw', 'regional_fuel_prices') }}
    WHERE FUEL_TYPE = 'REGULAR_GASOLINE'
      AND PRICE IS NOT NULL
),

national AS (
    SELECT
        WEEK_DATE,
        REGULAR_GASOLINE_PRICE AS NATIONAL_PRICE
    FROM {{ source('raw', 'fuel_prices') }}
    WHERE REGION = 'US_NATIONAL'
      AND REGULAR_GASOLINE_PRICE IS NOT NULL
),

joined AS (
    SELECT
        r.WEEK_DATE,
        r.REGION,
        r.REGIONAL_PRICE,
        n.NATIONAL_PRICE,
        ROUND(r.REGIONAL_PRICE - n.NATIONAL_PRICE, 4) AS PRICE_SPREAD,
        CASE
            WHEN n.NATIONAL_PRICE > 0
            THEN ROUND(
                ((r.REGIONAL_PRICE - n.NATIONAL_PRICE) / n.NATIONAL_PRICE) * 100,
                2)
            ELSE NULL
        END AS PRICE_SPREAD_PCT
    FROM regional r
    LEFT JOIN national n ON r.WEEK_DATE = n.WEEK_DATE
)

SELECT
    WEEK_DATE,
    REGION,
    REGIONAL_PRICE,
    NATIONAL_PRICE,
    PRICE_SPREAD,
    PRICE_SPREAD_PCT,

    -- 4-week rolling average regional price
    ROUND(AVG(REGIONAL_PRICE) OVER (
        PARTITION BY REGION ORDER BY WEEK_DATE
        ROWS BETWEEN 3 PRECEDING AND CURRENT ROW
    ), 4) AS REGIONAL_4WK_AVG,

    -- Running maximum spread seen so far for this region
    ROUND(MAX(PRICE_SPREAD) OVER (
        PARTITION BY REGION ORDER BY WEEK_DATE
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ), 4) AS MAX_SPREAD_SEEN,

    -- Price category label
    CASE
        WHEN PRICE_SPREAD >  0.30 THEN 'Significantly Above Average'
        WHEN PRICE_SPREAD >  0.10 THEN 'Above Average'
        WHEN PRICE_SPREAD > -0.10 THEN 'Near National Average'
        WHEN PRICE_SPREAD > -0.30 THEN 'Below Average'
        ELSE 'Significantly Below Average'
    END AS PRICE_CATEGORY

FROM joined
ORDER BY WEEK_DATE, REGION
