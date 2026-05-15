# FuelIQ Analytics - End-to-End Fuel Price Analytics System

An automated, end-to-end pipeline that ingests real-world fuel price data, transforms it with dbt, forecasts future prices using Snowflake ML, and visualizes everything in an interactive Preset dashboard.

---

## Table of Contents

1. [Project Overview](#1-project-overview)
2. [Questions This System Answers](#2-questions-this-system-answers)
3. [Tech Stack](#3-tech-stack)
4. [Data Sources](#4-data-sources)
5. [System Architecture](#5-system-architecture)
6. [Airflow Pipelines (DAGs)](#6-airflow-pipelines-dags)
7. [Database & Table Structures](#7-database--table-structures)
8. [dbt Transformation Models](#8-dbt-transformation-models)
9. [Snowflake ML Forecasting](#9-snowflake-ml-forecasting)
10. [Dashboard (Preset)](#10-dashboard-preset)
11. [Results & Key Findings](#11-results--key-findings)
12. [Model Evaluation Metrics](#12-model-evaluation-metrics)
13. [How to Run This Project](#13-how-to-run-this-project)
14. [Lessons Learned](#14-lessons-learned)
15. [Future Work](#15-future-work)
16. [References](#16-references)

---

## 1. Project Overview

Fuel prices affect households, logistics, retail, aviation, and supply chain operations. This project builds a **complete, automated analytics workflow** that answers: *where are prices going, which regions are expensive, and why are prices changing?*

The system:
- **Ingests** weekly national + regional fuel prices from the U.S. EIA API
- **Ingests** daily energy market data (crude oil, ETFs) from Yahoo Finance
- **Transforms** raw data into analytics-ready models using dbt
- **Forecasts** 12-week gasoline prices using Snowflake ML
- **Visualizes** everything in an interactive Preset dashboard

---

## 2. Questions This System Answers

| # | Analytical Question |
|---|---|
| 1 | How are U.S. gasoline and diesel prices changing over time? |
| 2 | Which U.S. regions are more expensive or cheaper than the national average? |
| 3 | How volatile are fuel prices from week to week? |
| 4 | How does WTI crude oil relate to retail gasoline prices? |
| 5 | What is the 12-week forecast for regular gasoline prices? |

---

## 3. Tech Stack

| Layer | Tool |
|---|---|
| Orchestration | Apache Airflow (Docker Compose, LocalExecutor) |
| Data Warehouse | Snowflake (USER_DB_FERRET / FERRET_QUERY_WH) |
| Data Sources | EIA Open Data API · Yahoo Finance (yfinance) |
| Transformation (ELT) | dbt (dbt-snowflake) |
| ML Forecasting | Snowflake ML Forecast |
| BI / Dashboard | Preset (Apache Superset) |
| Containerization | Docker |
| Language | Python 3 |

---

## 4. Data Sources

### Dataset 1 - EIA Open Data API (Historical Fuel Prices)

- **Provider:** U.S. Energy Information Administration  
- **Endpoint:** `https://api.eia.gov/v2/petroleum/pri/gnd/data/`  
- **Update frequency:** Weekly (every Wednesday)  
- **Auth:** EIA API key stored as Airflow Variable `eia_api_key`

**National Price Series** - latest 200 weekly records:

| Series ID | Fuel Type |
|---|---|
| EMM_EPMR_PTE_NUS_DPG | Regular Gasoline |
| EMM_EPMM_PTE_NUS_DPG | Midgrade Gasoline |
| EMM_EPMP_PTE_NUS_DPG | Premium Gasoline |
| EMD_EPD2D_PTE_NUS_DPG | Diesel |

**Regional Price Series** - latest 104 weekly records per region:

| Series ID | Region |
|---|---|
| EMM_EPMR_PTE_R10_DPG | East Coast |
| EMM_EPMR_PTE_R20_DPG | Midwest |
| EMM_EPMR_PTE_R30_DPG | Gulf Coast |
| EMM_EPMR_PTE_R40_DPG | Rocky Mountain |
| EMM_EPMR_PTE_R50_DPG | West Coast |
| EMM_EPMR_PTE_SCA_DPG | California |

**EIA Data Summary:**

| Data Type | Granularity | Records | Snowflake Target |
|---|---|---|---|
| National Prices | 4 fuel types | 200 | RAW.FUEL_PRICES |
| Regional Prices | 6 PADD regions | 104/region | RAW.REGIONAL_FUEL_PRICES |

---

### Dataset 2 - Yahoo Finance via yfinance (Real-Time Market Data)

- **Provider:** Yahoo Finance (via `yfinance` Python library)  
- **Update frequency:** Daily after market close  
- **History pulled:** 4 years of weekly OHLCV data

| Ticker | Full Name | Role |
|---|---|---|
| CL=F | WTI Crude Oil Futures | Primary U.S. crude benchmark - key cost driver for gasoline |
| BZ=F | Brent Crude Oil Futures | Global crude benchmark |
| XLE | Energy Select Sector ETF | Tracks ExxonMobil, Chevron, and major energy companies |
| UGA | United States Gasoline Fund ETF | Direct market proxy for pump price direction |

> **Why CL=F?** Crude oil is the primary raw material for refining gasoline. When WTI prices rise, retail pump prices follow within 1–3 weeks. The `dbt` model `CRUDE_CORRELATION` uses a 2-week lag to quantify this relationship.

---

## 5. System Architecture

The system has six layers: **Source → Warehouse (RAW) → ELT (dbt) → Forecasting (Snowflake ML) → Analytics → BI (Preset)**.

<img width="1282" height="794" alt="image" src="https://github.com/user-attachments/assets/f9334bf6-8710-4cfe-9f1e-b2e30e0a57b4" />


*Figure 1: Full end-to-end system architecture*

---

## 6. Airflow Pipelines (DAGs)

Four DAGs run on a staggered schedule so each pipeline always runs after its upstream dependency completes.

<img width="1280" height="404" alt="image" src="https://github.com/user-attachments/assets/88f7a834-3acb-40c4-ab7f-53a4697fb2cd" />


*Figure 2: All four DAG pipelines shown in the Airflow UI*

### DAG Execution Schedule

| DAG ID | Schedule | UTC Time | Depends On |
|---|---|---|---|
| FuelPrice_EIA_ETL | Weekly | Wed 06:00 | EIA API available |
| FuelPrice_Realtime_ETL | Daily | 08:00 | Yahoo Finance market close |
| FuelPrice_TrainPredict | Daily | 09:00 | FuelPrice_EIA_ETL must have loaded RAW.FUEL_PRICES |
| FuelPrice_DBT | Daily | 10:00 | Both ETL DAGs must have completed |

### End-to-End Data Flow

| DAG | Data Source | Snowflake Table(s) Loaded | Type |
|---|---|---|---|
| FuelPrice_EIA_ETL | EIA API | RAW.FUEL_PRICES | Historical / Archive |
| FuelPrice_EIA_ETL | EIA API | RAW.REGIONAL_FUEL_PRICES | Historical / Regional |
| FuelPrice_Realtime_ETL | Yahoo Finance (yfinance) | RAW.ENERGY_MARKET_PRICES | Current / Near-Real-Time |
| FuelPrice_TrainPredict | RAW.FUEL_PRICES | ANALYTICS.FUEL_PRICE_FINAL | ML Forecast Output |
| FuelPrice_DBT | RAW Tables | DBT.PRICE_MOVING_AVG | ELT Analytics |
| FuelPrice_DBT | RAW Tables | DBT.PRICE_VOLATILITY | ELT Analytics |
| FuelPrice_DBT | RAW Tables | DBT.CRUDE_CORRELATION | ELT Analytics |
| FuelPrice_DBT | RAW Tables | DBT.REGIONAL_COMPARISON | ELT Analytics |
| FuelPrice_DBT | RAW.FUEL_PRICES | DBT.FUEL_PRICES_SNAPSHOT | SCD Type 2 Snapshot |

---

### Pipeline 1 - FuelPrice_EIA_ETL

**What it does:**
- Extracts national weekly gasoline + diesel prices from the EIA API
- Extracts regional gasoline prices for all U.S. PADD regions
- Transforms API responses into structured Snowflake records
- Loads into `RAW.FUEL_PRICES` and `RAW.REGIONAL_FUEL_PRICES`
- Uses **MERGE / upsert** strategy to prevent duplicates

<img width="1048" height="564" alt="image" src="https://github.com/user-attachments/assets/dff8c3b3-719b-4f4b-81e3-9b6cbac18c95" />


<img width="1312" height="786" alt="image" src="https://github.com/user-attachments/assets/29aad222-20e0-461e-a7c1-8d20fda6a568" />


*Figure 3: Detailed view of Pipeline 1 (FuelPrice_EIA_ETL)*




---

### Pipeline 2 - FuelPrice_Realtime_ETL

**What it does:**
- Extracts energy market data from Yahoo Finance using `yfinance`
- Pulls weekly OHLCV data for CL=F, BZ=F, XLE, and UGA
- Standardizes ticker names and aligns dates with weekly fuel records
- Loads into `RAW.ENERGY_MARKET_PRICES`

<img width="1080" height="534" alt="image" src="https://github.com/user-attachments/assets/aeb0cff0-f618-4c17-93b9-3f591f80fb49" />


*Figure 5: Detailed view of Pipeline 2 (FuelPrice_Realtime_ETL)*

<img width="1228" height="356" alt="image" src="https://github.com/user-attachments/assets/67871072-9de2-45ba-8691-e68a44059514" />


*Figure 6: Airflow execution log for FuelPrice_Realtime_ETL*

---

### Pipeline 3 - FuelPrice_TrainPredict

**What it does:**
- Creates ML training view `ADHOC.FUEL_PRICE_TRAIN_VIEW` from `RAW.FUEL_PRICES`
- Trains a `SNOWFLAKE.ML.FORECAST` model on regular gasoline price history
- Generates 12-week forecast with 95% prediction interval
- Stores raw forecast in `ADHOC.FUEL_PRICE_FORECAST`
- Combines historical + forecasted data into `ANALYTICS.FUEL_PRICE_FINAL`
- Stores evaluation metrics in `ANALYTICS.FUEL_PRICE_MODEL_METRICS`

<img width="846" height="506" alt="image" src="https://github.com/user-attachments/assets/7c4bc070-db2b-4f9d-9315-ab5ee939be81" />

<img width="1142" height="352" alt="image" src="https://github.com/user-attachments/assets/3c2cafcc-804b-46ea-93c8-2218ae81881e" />


*Figure 7: Detailed view of Pipeline 3 (FuelPrice_TrainPredict)*

---

### Pipeline 4 - FuelPrice_DBT

**What it does:**
- Runs all four dbt transformation models
- Runs 31 schema tests (not_null + accepted_values)
- Updates the SCD Type 2 snapshot

---

### Airflow Credentials Setup

Snowflake credentials are stored securely as an **Airflow Connection** (`snowflake_conn`). No credentials are hardcoded in any script.

<img width="1164" height="650" alt="image" src="https://github.com/user-attachments/assets/5a5cc6f1-ab6d-4f27-adb1-2cb6b5b51f29" />

*Figure 8: Snowflake credentials in the snowflake_conn Airflow Connection*

The EIA API key is stored as an **Airflow Variable** (`eia_api_key`).


<img width="1318" height="394" alt="image" src="https://github.com/user-attachments/assets/4c0ed083-0b23-4325-a455-60b298954187" />

*Figure 9: EIA API key configured as Airflow Variable*

---

## 7. Database & Table Structures

All tables live in Snowflake database `USER_DB_FERRET`, warehouse `FERRET_QUERY_WH`.

### Schema Overview

| Schema | Purpose |
|---|---|
| RAW | Raw source data loaded by Airflow ETL DAGs |
| ADHOC | Intermediate ML training objects and raw forecast output |
| ANALYTICS | Final forecast and model metric tables ready for BI |
| DBT | dbt-managed ELT transformation tables + SCD Type 2 snapshot |

---

### RAW Schema

#### RAW.FUEL_PRICES

| Column | Type | Constraint | Description |
|---|---|---|---|
| WEEK_DATE | DATE | NOT NULL, PK | Week the price was recorded |
| REGION | STRING | NOT NULL, PK | Always US_NATIONAL |
| REGULAR_GASOLINE_PRICE | FLOAT | - | Weekly U.S. regular gasoline price (USD/gal) |
| MIDGRADE_GASOLINE_PRICE | FLOAT | - | Weekly midgrade price (USD/gal) |
| PREMIUM_GASOLINE_PRICE | FLOAT | - | Weekly premium price (USD/gal) |
| DIESEL_PRICE | FLOAT | - | Weekly diesel price (USD/gal) |
| PRICE_UNIT | STRING | - | USD_PER_GALLON |
| SOURCE | STRING | - | EIA_WEEKLY |
| LOAD_TS | TIMESTAMP | DEFAULT NOW() | Row load/update timestamp |

<img width="912" height="508" alt="image" src="https://github.com/user-attachments/assets/c5b11b04-916f-48b0-93ff-0a8c19bf8cec" />


*Figure: Snowflake output showing populated RAW.FUEL_PRICES*

---

#### RAW.REGIONAL_FUEL_PRICES

| Column | Type | Constraint | Description |
|---|---|---|---|
| WEEK_DATE | DATE | NOT NULL, PK | Week the price was recorded |
| REGION | STRING | NOT NULL, PK | PADD region (e.g., EAST_COAST, CALIFORNIA) |
| PRICE | FLOAT | NOT NULL | Weekly regional regular gasoline price (USD/gal) |
| FUEL_TYPE | STRING | - | REGULAR_GASOLINE |
| PRICE_UNIT | STRING | - | USD_PER_GALLON |
| SOURCE | STRING | - | EIA_REGIONAL |
| LOAD_TS | TIMESTAMP | DEFAULT NOW() | Row load/update timestamp |

<img width="1044" height="556" alt="image" src="https://github.com/user-attachments/assets/bcbe1aa1-25ed-492a-a3ec-233649453667" />


*Figure: Snowflake output showing populated RAW.REGIONAL_FUEL_PRICES*

---

#### RAW.ENERGY_MARKET_PRICES

| Column | Type | Constraint | Description |
|---|---|---|---|
| WEEK_DATE | DATE | NOT NULL, PK | Week the market data was recorded |
| TICKER | STRING | NOT NULL, PK | CL=F, BZ=F, XLE, or UGA |
| TICKER_NAME | STRING | - | Human-readable ticker name |
| OPEN_PRICE | FLOAT | - | Opening price for the week |
| HIGH_PRICE | FLOAT | - | Highest price during the week |
| LOW_PRICE | FLOAT | - | Lowest price during the week |
| CLOSE_PRICE | FLOAT | NOT NULL | Closing price for the week |
| VOLUME | BIGINT | - | Weekly trading volume |
| LOAD_TS | TIMESTAMP | DEFAULT NOW() | Row load/update timestamp |

<img width="1042" height="574" alt="image" src="https://github.com/user-attachments/assets/4dc0e089-978c-4d73-b25f-d27a86edc10d" />


*Figure: Snowflake output showing populated RAW.ENERGY_MARKET_PRICES*

---

### ADHOC Schema

| Table / View | Created By | Purpose |
|---|---|---|
| ADHOC.FUEL_PRICE_TRAIN_VIEW | FuelPrice_TrainPredict | Clean ML training view (REGION, WEEK_DATE, REGULAR_GASOLINE_PRICE) |
| ADHOC.FUEL_PRICE_FORECAST | FuelPrice_TrainPredict | Raw ML forecast output via RESULT_SCAN(LAST_QUERY_ID()) |

---

### ANALYTICS Schema

#### ANALYTICS.FUEL_PRICE_FINAL (Primary Dashboard Source)

| Column | Type | Description |
|---|---|---|
| REGION | STRING | Always US_NATIONAL |
| WEEK_DATE | DATE | Historical or forecast date |
| ACTUAL | FLOAT | Historical actual price (NULL for forecast rows) |
| FORECAST | FLOAT | Forecast price (NULL for historical rows) |
| LOWER_BOUND | FLOAT | Lower 95% prediction interval |
| UPPER_BOUND | FLOAT | Upper 95% prediction interval |
| RECORD_TYPE | STRING | HISTORICAL or FORECAST |

<img width="1284" height="720" alt="image" src="https://github.com/user-attachments/assets/5dbe150a-53d2-4f9b-a088-2f8ecac1ee16" />


*Figure: Snowflake output showing populated ANALYTICS.FUEL_PRICE_FINAL*

#### ANALYTICS.FUEL_PRICE_MODEL_METRICS

| Column | Type | Description |
|---|---|---|
| RUN_TS | TIMESTAMP | Timestamp of the training run |
| SERIES | STRING | Series identifier (REGION value) |
| METRIC | STRING | MAE, MAPE, SMAPE, MSE, etc. |
| VALUE | FLOAT | Metric value for that training run |

---

### Complete Snowflake Table Inventory

| Schema | Table / View | Rows | Populated By |
|---|---|---|---|
| RAW | FUEL_PRICES | 200 | FuelPrice_EIA_ETL |
| RAW | REGIONAL_FUEL_PRICES | 624 | FuelPrice_Realtime |
| RAW | ENERGY_MARKET_PRICES | ~840 | FuelPrice_Realtime |
| ADHOC | FUEL_PRICE_TRAIN_VIEW | 200 | FuelPrice_Train |
| ADHOC | FUEL_PRICE_FORECAST | 12 | FuelPrice_Train |
| ANALYTICS | FUEL_PRICE_FINAL | 212 | FuelPrice_Train |
| ANALYTICS | FUEL_PRICE_MODEL_METRICS | 1/run | FuelPrice_Train |
| DBT | PRICE_MOVING_AVG | 200 | FuelPrice_DBT |
| DBT | PRICE_VOLATILITY | 200 | FuelPrice_DBT |
| DBT | CRUDE_CORRELATION | 200 | FuelPrice_DBT |
| DBT | REGIONAL_COMPARISON | 624 | FuelPrice_DBT |
| DBT | FUEL_PRICES_SNAPSHOT | 200 | FuelPrice_DBT |

---

## 8. dbt Transformation Models

The `FuelPrice_DBT` DAG runs four dbt models and one SCD Type 2 snapshot.

### Model 1 - DBT.PRICE_MOVING_AVG

**Purpose:** Rolling fuel price averages to smooth short-term noise and identify long-term trends.

| Column | Description |
|---|---|
| WEEK_DATE | Date of observation |
| REGULAR_4WK_AVG | 4-week rolling average (regular gasoline) |
| DIESEL_4WK_AVG | 4-week rolling average (diesel) |
| REGULAR_12WK_AVG | 12-week rolling average (regular gasoline) |
| DIESEL_12WK_AVG | 12-week rolling average (diesel) |
| REGULAR_52WK_AVG | 52-week rolling average (regular gasoline) |
| DIESEL_52WK_AVG | 52-week rolling average (diesel) |
| PREMIUM_SPREAD | Premium price minus regular price |
| DIESEL_SPREAD | Diesel price minus regular price |

<img width="1290" height="736" alt="image" src="https://github.com/user-attachments/assets/35f8287a-199e-4a4b-9165-3e7e74c77a29" />


*Figure: Snowflake output showing populated DBT.PRICE_MOVING_AVG*

---

### Model 2 - DBT.PRICE_VOLATILITY

**Purpose:** Week-over-week changes, rolling standard deviation, and directional labels.

| Column | Description |
|---|---|
| REGULAR_WOW_CHANGE | Dollar change week over week |
| REGULAR_WOW_PCT_CHANGE | % change week over week |
| HIST_AVG_REGULAR | All-time historical average |
| PRICE_ANOMALY | Deviation from historical average |
| ROLLING_12WK_STDDEV | 12-week rolling standard deviation |
| PRICE_DIRECTION | Rising / Falling / Stable |
| VOLATILITY_LABEL | High / Moderate / Low Volatility |

<img width="1290" height="722" alt="image" src="https://github.com/user-attachments/assets/a98fcec9-f52c-4243-a753-315e8c70c7b1" />


*Figure: Snowflake output showing populated DBT.PRICE_VOLATILITY*

---

### Model 3 - DBT.CRUDE_CORRELATION

**Purpose:** Join retail gasoline prices with WTI crude oil futures using a 2-week lag.

| Column | Description |
|---|---|
| CRUDE_CLOSE_USD_BBL | WTI crude closing price (USD/barrel) |
| CRUDE_USD_PER_GALLON | WTI crude price converted to USD/gallon (÷42) |
| CRUDE_LAG2_BBL | WTI price 2 weeks prior (USD/barrel) |
| CRUDE_LAG2_GAL | WTI price 2 weeks prior (USD/gallon) |
| PUMP_TO_CRUDE_MARGIN | Retail gasoline minus lagged crude cost |
| MARGIN_CATEGORY | Wide Margin / Normal Margin / Tight Margin |

> **Why a 2-week lag?** Crude oil prices typically take 1–3 weeks to pass through refining and distribution before appearing at the pump. The lag makes the correlation analytically accurate.

<img width="1262" height="710" alt="image" src="https://github.com/user-attachments/assets/e6674809-5b49-4ec0-a5f1-9910638e50e8" />


*Figure: Snowflake output showing populated DBT.CRUDE_CORRELATION*

---

### Model 4 - DBT.REGIONAL_COMPARISON

**Purpose:** Compare each PADD region's price against the U.S. national average.

| Column | Description |
|---|---|
| REGIONAL_PRICE | Weekly regional gasoline price (USD/gal) |
| NATIONAL_PRICE | U.S. national weekly average (USD/gal) |
| PRICE_SPREAD | Regional minus national price |
| PRICE_SPREAD_PCT | % spread relative to national average |
| REGIONAL_4WK_AVG | 4-week rolling average of regional price |
| MAX_SPREAD_SEEN | Running maximum spread for this region |
| PRICE_CATEGORY | Significantly Above / Above / Near / Below / Significantly Below Average |

<img width="1264" height="704" alt="image" src="https://github.com/user-attachments/assets/bd5963de-c07d-4879-a384-72ccbce82b3e" />


*Figure: Snowflake output showing populated DBT.REGIONAL_COMPARISON*

---

### Model 5 - DBT.FUEL_PRICES_SNAPSHOT (SCD Type 2)

**Purpose:** Track every historical change to `RAW.FUEL_PRICES` over time using Slowly Changing Dimension Type 2.

<img width="1266" height="720" alt="image" src="https://github.com/user-attachments/assets/57123106-1c73-4042-9be2-77836b3947ec" />

Key SCD columns:

| Column | Description |
|---|---|
| DBT_SCD_ID | dbt-generated unique row identifier |
| DBT_VALID_FROM | Start of validity for this row version |
| DBT_VALID_TO | End of validity (NULL = current record) |
| DBT_IS_CURRENT_RECORD | TRUE if this is the active record for that key |

---

### dbt Run Results

All four models built successfully:

<img width="1272" height="342" alt="image" src="https://github.com/user-attachments/assets/d32f711c-d5f9-4bfb-820e-727d01e0b8e2" />


*Figure: dbt run output - 4/4 models SUCCESS, WARN=0, ERROR=0*

---

### dbt Test Results - 31/31 PASS

Tests covered:
- `not_null` constraints on all key columns across all models and RAW sources
- `accepted_values` validations for PRICE_DIRECTION, VOLATILITY_LABEL, MARGIN_CATEGORY, PRICE_CATEGORY, TICKER, REGION, FUEL_TYPE

| Model / Source | Test Type | Columns Tested |
|---|---|---|
| RAW source (fuel_prices) | not_null | WEEK_DATE, REGION, REGULAR_GASOLINE_PRICE |
| RAW source (regional_fuel_prices) | not_null + accepted_values | WEEK_DATE, REGION, PRICE; 6 PADD values |
| RAW source (energy_market_prices) | not_null + accepted_values | WEEK_DATE, TICKER, CLOSE_PRICE; 4 tickers |
| price_moving_avg | not_null | WEEK_DATE, REGION, REGULAR_GASOLINE_PRICE, averages |
| price_volatility | not_null + accepted_values | WEEK_DATE, REGION; Rising/Falling/Stable, 3 volatility tiers |
| crude_correlation | not_null + accepted_values | WEEK_DATE, REGULAR_GASOLINE_PRICE; margin categories |
| regional_comparison | not_null + accepted_values | WEEK_DATE, REGION, REGIONAL_PRICE; 5 price category labels |

<img width="1268" height="902" alt="image" src="https://github.com/user-attachments/assets/e10373eb-c102-437d-afa7-8da517f6fbc6" />

<img width="1288" height="888" alt="image" src="https://github.com/user-attachments/assets/69479d2b-c88f-448b-80da-6c1b38daac04" />

*Figure: dbt test output - 31/31 PASS, WARN=0, ERROR=0*

---

### dbt Snapshot Result


<img width="1298" height="294" alt="image" src="https://github.com/user-attachments/assets/c15daf8a-b8b0-4a30-8860-8e3d4859e875" />


*Figure: dbt snapshot - PASS=1, WARN=0, ERROR=0*

---

## 9. Snowflake ML Forecasting

### How It Works - Step by Step

```
Step 1: Create Training View
   ADHOC.FUEL_PRICE_TRAIN_VIEW
   → Columns: REGION, WEEK_DATE, REGULAR_GASOLINE_PRICE
   → Filter: US_NATIONAL series only

Step 2: Train Forecast Model
   SNOWFLAKE.ML.FORECAST
   → Series column: REGION
   → Timestamp column: WEEK_DATE
   → Target: REGULAR_GASOLINE_PRICE
   → Config: ON_ERROR: SKIP

Step 3: Generate 12-Week Forecast
   → Captures output with RESULT_SCAN(LAST_QUERY_ID())
   → Stores in ADHOC.FUEL_PRICE_FORECAST
   → Includes: FORECAST value, LOWER_BOUND, UPPER_BOUND (95% CI)

Step 4: Build Final Analytics Table
   ANALYTICS.FUEL_PRICE_FINAL
   → UNION ALL of historical actuals + 12 forecast rows
   → RECORD_TYPE column: 'HISTORICAL' or 'FORECAST'
```

>  **Important:** Snowflake ML Forecast output must be captured with `RESULT_SCAN(LAST_QUERY_ID())` in the **same session** immediately after the `CALL` statement. This is a non-obvious Snowflake requirement.

### Forecast Summary

| Parameter | Value |
|---|---|
| Training data range | July 11, 2022 – May 4, 2026 |
| Training rows | 200 weekly records |
| Forecast horizon | 12 weeks |
| Forecast date range | May 11, 2026 – July 27, 2026 |
| Confidence level | 95% |
| Point forecast range | ~$3.75 – $4.05 per gallon |
| Latest actual price | $4.452/gallon (2026-05-04) |

---

## 10. Dashboard (Preset)

### Connection Setup

The Preset dashboard connects directly to Snowflake `USER_DB_FERRET`:

| Parameter | Value |
|---|---|
| Database | USER_DB_FERRET |
| Account | SFEDU02-EAB27764 |
| Warehouse | FERRET_QUERY_WH |
| Role | TRAINING_ROLE |

<img width="472" height="746" alt="image" src="https://github.com/user-attachments/assets/10e4980f-0f17-4d2d-8709-0a5909d8bd6a" />


*Figure: Preset – Snowflake connection setup*

---

### Dashboard Data Sources

| Dataset | What It Powers |
|---|---|
| ANALYTICS.FUEL_PRICE_FINAL | National gasoline trend + 12-week forecast with confidence intervals |
| DBT.PRICE_MOVING_AVG | 4-week, 12-week, 52-week rolling average price trends |
| DBT.PRICE_VOLATILITY | Week-over-week volatility, price direction, anomaly detection |
| DBT.CRUDE_CORRELATION | WTI crude vs. retail gasoline spread and margin analysis |
| DBT.REGIONAL_COMPARISON | Regional price comparison vs. national average |
| RAW.ENERGY_MARKET_PRICES | Energy market ticker overview (CL=F, BZ=F, XLE, UGA) |

---

### Primary Dashboard

**Link:** https://c46c4a3f.us2a.app.preset.io/superset/dashboard/9/?native_filters_key=kfF5vBzGmWA


<img width="3398" height="1872" alt="image" src="https://github.com/user-attachments/assets/fb7a8338-e33e-4355-92ac-51e9b5d9d227" />


*Figure: Preset Dashboard - Primary View (Fuel Analysis Dashboard)*

The primary dashboard includes:
- **Gauge chart** - latest national regular gasoline price
- **Forecast card** - predicted future gasoline price (~$4.01/gallon)
- **Actual vs. Forecast chart** - historical values vs. 12-week ML forecast
- **Crude oil scatterplot** - positive relationship between crude and pump prices
- **Regional comparison chart** - California and West Coast consistently above national average

---

### Secondary Dashboard

**Link:** https://98876585.us1a.app.preset.io/superset/dashboard/8/

<img width="3416" height="1856" alt="image" src="https://github.com/user-attachments/assets/62330383-60cd-4261-b0cd-2a05a75d02e4" />


*Figure: Preset Dashboard - Secondary View (Regional + Correlation)*

The secondary dashboard includes:
- National fuel price trends
- Regional price boxplots (spread and outliers by PADD region)
- Crude oil price comparison and scatterplot
- Volatility chart (shows recent sharp increase in price volatility)

---

## 11. Results & Key Findings

### Pipeline Execution 

All four Airflow DAGs executed successfully with no failures or retries:


*Figure: All four Airflow DAGs running successfully*

---

### Key Findings from the Dashboard

#### Finding 1 - Long-Term Upward Trend in U.S. Fuel Prices

U.S. regular gasoline trended from ~$2.78/gallon in late 2022 to a peak of ~$4.65/gallon in mid-2022, then stabilized in the $3.50–$4.50 range through 2025–2026. The 52-week rolling average clearly reveals this structural upward trend.

---

#### Finding 2 - Persistent Regional Price Disparities

| Region | vs. National Average |
|---|---|
| California | +31% to +40% (Significantly Above Average) |
| West Coast | +25% above national average |
| Gulf Coast | 10–15% below national average (cheapest) |
| Rocky Mountain / Midwest | Near national average |

California's premium reflects its unique fuel blend requirements, higher state taxes, geographic isolation, and refinery capacity constraints.

---

#### Finding 3 - Low Background Volatility with Episodic Spikes

- **91.35%** of weekly observations → Low Volatility
- **7.69%** → High Volatility
- High-volatility periods correspond to geopolitical supply disruptions and refinery events

---

#### Finding 4 - Positive but Lagged Crude Oil Correlation

- `PUMP_TO_CRUDE_MARGIN` ranges from ~$1.35 (Tight Margin) to ~$2.20+ (Wide Margin) per gallon
- Wide Margin periods occur when crude prices spike and retail prices lag
- The 2-week lag model confirms the industry-observed refinery-to-retail lag window

---

#### Finding 5 - Forecast Shows Moderate Price Moderation

- Snowflake ML projects gasoline prices declining from $4.45/gallon toward ~$3.75–$4.05/gallon over 12 weeks
- 95% confidence bounds are relatively narrow - moderate forecast certainty
- **Note:** External shocks (geopolitical events, refinery outages) are not modeled

---

## 12. Model Evaluation Metrics

Training run: **May 7, 2026 at 02:01:44 UTC** | Series: **US_NATIONAL**

| Metric | Value | Interpretation |
|---|---|---|
| MAE | 0.295 | Average forecast error of ~$0.30 per gallon |
| MAPE | 0.080 | Average % error of ~8.0% |
| SMAPE | 0.087 | Symmetric % error of ~8.7% |
| MSE | 0.235 | Squared error (sensitive to large misses) |
| MDA | 0.462 | Correctly predicts price direction 46.2% of the time |
| Coverage (95% CI) | 0.775 | 77.5% of actual values fall within the 95% interval |
| Winkler Score (α=0.05) | 5.335 | Lower = tighter and more accurate intervals |

**Overall assessment:** The model estimates price levels well (MAE, MAPE) but is less reliable for predicting weekly direction (MDA = 46.2%). Future improvements: include crude oil prices, regional indicators, and macroeconomic variables as exogenous features.

---

## 13. How to Run This Project

### Prerequisites

- Docker + Docker Compose installed
- Snowflake account with `USER_DB_FERRET` database and `FERRET_QUERY_WH` warehouse
- EIA API key (free at [eia.gov](https://www.eia.gov/opendata/))

### Step 1 - Clone the Repository

```bash
git clone https://github.com/Kshitija-Shinde9/End-to-End-Dbt-Airflow-Snowfalke-Data-Pipeline-Fuel-Price_Analysis
cd End-to-End-Dbt-Airflow-Snowfalke-Data-Pipeline-Fuel-Price_Analysis
```

### Step 2 - Start Airflow with Docker

```bash
docker-compose up -d
```

Access the Airflow UI at: `http://localhost:8080`

### Step 3 - Configure Airflow Credentials

**Airflow Connection** (`Admin → Connections → Add`):

| Field | Value |
|---|---|
| Connection ID | `snowflake_conn` |
| Connection Type | Snowflake |
| Account | Your Snowflake account |
| Database | USER_DB_FERRET |
| Warehouse | FERRET_QUERY_WH |
| Role | TRAINING_ROLE |
| Schema | RAW |

**Airflow Variable** (`Admin → Variables → Add`):

| Key | Value |
|---|---|
| `eia_api_key` | Your EIA API key |

### Step 4 - Run the DAGs (in order)

```
1. FuelPrice_EIA_ETL       → loads RAW.FUEL_PRICES + RAW.REGIONAL_FUEL_PRICES
2. FuelPrice_Realtime_ETL  → loads RAW.ENERGY_MARKET_PRICES
3. FuelPrice_TrainPredict  → trains ML model, generates ANALYTICS.FUEL_PRICE_FINAL
4. FuelPrice_DBT           → builds dbt models + runs 31 tests + updates snapshot
```

### Step 5 - Verify Data in Snowflake

```sql
-- Check all tables are populated
SELECT COUNT(*) FROM USER_DB_FERRET.RAW.FUEL_PRICES;               -- expect ~200
SELECT COUNT(*) FROM USER_DB_FERRET.RAW.REGIONAL_FUEL_PRICES;      -- expect ~624
SELECT COUNT(*) FROM USER_DB_FERRET.RAW.ENERGY_MARKET_PRICES;      -- expect ~840
SELECT COUNT(*) FROM USER_DB_FERRET.ANALYTICS.FUEL_PRICE_FINAL;    -- expect 212
SELECT COUNT(*) FROM USER_DB_FERRET.DBT.PRICE_MOVING_AVG;          -- expect ~200
SELECT COUNT(*) FROM USER_DB_FERRET.DBT.PRICE_VOLATILITY;          -- expect ~200
SELECT COUNT(*) FROM USER_DB_FERRET.DBT.CRUDE_CORRELATION;         -- expect ~200
SELECT COUNT(*) FROM USER_DB_FERRET.DBT.REGIONAL_COMPARISON;       -- expect ~624

-- Check ticker distribution
SELECT TICKER, COUNT(*)
FROM USER_DB_FERRET.RAW.ENERGY_MARKET_PRICES
GROUP BY TICKER ORDER BY TICKER;
```

### Step 6 - Connect Preset to Snowflake

1. Go to [Preset](https://preset.io) and create a new database connection
2. Use: Database = `USER_DB_FERRET`, Warehouse = `FERRET_QUERY_WH`, Role = `TRAINING_ROLE`
3. Import the dashboard from the repository or build charts from the tables listed above

---

## 14. Lessons Learned

### 1. RESULT_SCAN Is Non-Negotiable for Snowflake ML
Snowflake ML Forecast returns output from a `CALL` statement. You **cannot** use `CREATE TABLE AS SELECT` to capture it. You must call `RESULT_SCAN(LAST_QUERY_ID())` in the **same session** immediately after the `CALL`. A different cursor = empty result.

### 2. Credential Injection via Environment Variables
Use Airflow Jinja templating (`{{ conn.snowflake_conn.password }}`) in `BashOperator env` dictionaries. Never hardcode credentials in `profiles.yml`. This keeps all secrets inside Airflow Connections, out of the codebase, and portable across environments.

### 3. dbt Full Path Inside Docker
When dbt is installed via `_PIP_ADDITIONAL_REQUIREMENTS`, it may not be on the system `PATH`. Always use the full absolute path:
```bash
/home/airflow/.local/bin/dbt run --profiles-dir /path/to/profiles
```

### 4. MERGE Over INSERT Ensures Idempotency
Plain `INSERT` creates duplicates on re-runs. `MERGE` with `NOT MATCHED / WHEN MATCHED` guarantees each `(WEEK_DATE, REGION)` or `(WEEK_DATE, TICKER)` pair appears **exactly once**, regardless of how many times the pipeline runs.

### 5. The 2-Week Crude Oil Lag Matters
A same-week join of crude and pump prices produces a superficial correlation. A `LAG(CRUDE_CLOSE_USD_BBL, 2)` produces a more accurate `PUMP_TO_CRUDE_MARGIN` that reflects real refinery-to-retail timing.

### 6. Free, Official Data Sources Are Production-Quality
The EIA API (free API key) + yfinance (no registration) together provide everything needed for all five analytical questions. Commercial subscriptions are not necessary.

---

## 15. Future Work

| Enhancement | Description |
|---|---|
| **Multivariate Forecasting** | Add WTI crude (CL=F) or gasoline futures (UGA) as exogenous features in `SNOWFLAKE.ML.FORECAST` for higher accuracy |
| **dbt Incremental Models** | Replace full-refresh materializations with incremental models to reduce compute cost as data grows |
| **Extended Historical Window** | Pull full EIA history (back to the 1990s) for more statistically reliable 52-week averages and seasonal ML patterns |
| **Cloud-Managed Orchestration** | Migrate from local Docker Airflow to Amazon MWAA, Google Cloud Composer, or Astronomer |
| **Station-Level Price Data** | Integrate GasBuddy API for ZIP-code-level price granularity |
| **Automated Alerting** | Add Airflow callbacks or Slack/email alerts when volatility spikes, prices cross thresholds, or any pipeline fails |

---

## 16. References

1. U.S. Energy Information Administration, "EIA Open Data API" - https://api.eia.gov/v2/petroleum/pri/gnd/data/
2. Yahoo Finance - https://finance.yahoo.com/
3. yfinance Python Library - https://pypi.org/project/yfinance/
4. Apache Airflow Documentation - https://airflow.apache.org/docs/
5. Snowflake Documentation - https://docs.snowflake.com/
6. Snowflake ML Forecast - https://docs.snowflake.com/en/developer-guide/snowflake-ml/forecast
7. dbt Documentation - https://docs.getdbt.com/
8. dbt Snapshots - https://docs.getdbt.com/docs/build/snapshots
9. Docker Documentation - https://docs.docker.com/
10. Python Documentation - https://docs.python.org/3/
