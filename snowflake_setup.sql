-- ============================================================
-- FUEL PRICE ANALYTICS — UPDATED SNOWFLAKE SETUP FOR FERRET
-- Database: USER_DB_FERRET
-- Warehouse: FERRET_QUERY_WH
-- Role: TRAINING_ROLE
-- ============================================================

USE ROLE TRAINING_ROLE;

-- Use your assigned database
USE DATABASE USER_DB_FERRET;

-- Use your assigned warehouse
USE WAREHOUSE FERRET_QUERY_WH;

-- ============================================================
-- STEP 1: CREATE SCHEMAS
-- ============================================================

CREATE SCHEMA IF NOT EXISTS RAW;
CREATE SCHEMA IF NOT EXISTS ADHOC;
CREATE SCHEMA IF NOT EXISTS ANALYTICS;
CREATE SCHEMA IF NOT EXISTS DBT;

-- ============================================================
-- STEP 2: RAW TABLES
-- ============================================================

USE SCHEMA RAW;

-- TABLE 1: National weekly fuel prices from EIA
-- Populated by: FuelPrice_EIA_ETL DAG
CREATE TABLE IF NOT EXISTS RAW.FUEL_PRICES (
    WEEK_DATE                DATE          NOT NULL,
    REGION                   STRING        NOT NULL DEFAULT 'US_NATIONAL',
    REGULAR_GASOLINE_PRICE   FLOAT,
    MIDGRADE_GASOLINE_PRICE  FLOAT,
    PREMIUM_GASOLINE_PRICE   FLOAT,
    DIESEL_PRICE             FLOAT,
    PRICE_UNIT               STRING        DEFAULT 'USD_PER_GALLON',
    SOURCE                   STRING        DEFAULT 'EIA_WEEKLY',
    LOAD_TS                  TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);


-- TABLE 2: Regional weekly fuel prices from EIA (by PADD region)
-- Populated by: FuelPrice_EIA_ETL DAG
CREATE TABLE IF NOT EXISTS RAW.REGIONAL_FUEL_PRICES (
    WEEK_DATE    DATE   NOT NULL,
    REGION       STRING NOT NULL,
    PRICE        FLOAT,
    FUEL_TYPE    STRING NOT NULL DEFAULT 'REGULAR_GASOLINE',
    PRICE_UNIT   STRING        DEFAULT 'USD_PER_GALLON',
    SOURCE       STRING        DEFAULT 'EIA_REGIONAL',
    LOAD_TS      TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);


-- TABLE 3: Energy market prices from Yahoo Finance (yfinance)
-- Tickers: CL=F (WTI Crude), BZ=F (Brent Crude), XLE (Energy ETF), UGA (Gas ETF)
-- Populated by: FuelPrice_Realtime_ETL DAG


CREATE TABLE IF NOT EXISTS RAW.ENERGY_MARKET_PRICES (
    WEEK_DATE    DATE   NOT NULL,
    TICKER       STRING NOT NULL,
    TICKER_NAME  STRING,
    OPEN_PRICE   FLOAT,
    HIGH_PRICE   FLOAT,
    LOW_PRICE    FLOAT,
    CLOSE_PRICE  FLOAT,
    VOLUME       BIGINT,
    LOAD_TS      TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);



-- ============================================================
-- STEP 3: ADHOC TABLES
-- ============================================================

-- TABLE 4: ML Training View (created/replaced by Airflow — defined here for reference)
-- This is a VIEW not a table, created by TrainPredict DAG at runtime

-- TABLE 5: Raw ML forecast output
-- Populated by: FuelPrice_TrainPredict DAG

USE SCHEMA ADHOC;

CREATE TABLE IF NOT EXISTS ADHOC.FUEL_PRICE_FORECAST (
    SERIES      STRING,
    TS          TIMESTAMP,
    FORECAST    FLOAT,
    LOWER_BOUND FLOAT,
    UPPER_BOUND FLOAT
);

-- ============================================================
-- STEP 4: ANALYTICS TABLES
-- ============================================================

USE SCHEMA ANALYTICS;


-- TABLE 6: Unified historical + forecast (UNION ALL)
-- Populated by: FuelPrice_TrainPredict DAG
CREATE TABLE IF NOT EXISTS ANALYTICS.FUEL_PRICE_FINAL (
    REGION       STRING,
    WEEK_DATE    DATE,
    ACTUAL       FLOAT,
    FORECAST     FLOAT,
    LOWER_BOUND  FLOAT,
    UPPER_BOUND  FLOAT,
    RECORD_TYPE  STRING
);

-- TABLE 7: ML model evaluation metrics
-- Populated by: FuelPrice_TrainPredict DAG (appended each run)

CREATE TABLE IF NOT EXISTS ANALYTICS.FUEL_PRICE_MODEL_METRICS (
    RUN_TS             TIMESTAMP_NTZ,
    SERIES             STRING,
    ERROR_METRIC       STRING,
    METRIC_VALUE       FLOAT,
    STANDARD_DEVIATION FLOAT,
    LOGS               VARIANT
);

-- ============================================================
-- STEP 8: DBT SCHEMA
-- (Tables created automatically by dbt run )
-- Listed here for reference only:
--   DBT.PRICE_MOVING_AVG     — rolling averages
--   DBT.PRICE_VOLATILITY     — week-over-week changes
--   DBT.CRUDE_CORRELATION    — crude oil vs pump price
--   DBT.REGIONAL_COMPARISON  — regional price spreads
--   DBT.FUEL_PRICES_SNAPSHOT — SCD Type 2 snapshot
-- ============================================================

USE SCHEMA DBT;

-- -----------------------------------------------
-- STEP 9: VERIFY EVERYTHING WAS CREATED
-- -----------------------------------------------

SHOW SCHEMAS IN DATABASE USER_DB_FERRET;

SHOW TABLES IN SCHEMA USER_DB_FERRET.RAW;
SHOW TABLES IN SCHEMA USER_DB_FERRET.ADHOC;
SHOW TABLES IN SCHEMA USER_DB_FERRET.ANALYTICS;
SHOW TABLES IN SCHEMA USER_DB_FERRET.DBT;

SHOW WAREHOUSES LIKE 'FERRET_QUERY_WH';























