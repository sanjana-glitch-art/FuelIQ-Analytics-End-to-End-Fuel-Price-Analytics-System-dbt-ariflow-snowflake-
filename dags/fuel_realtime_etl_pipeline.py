"""
fuel_realtime_etl_pipeline.py  - DAG 2 of 4
================================================
DATA SOURCE:
  Yahoo Finance via yfinance library
  → RAW.ENERGY_MARKET_PRICES

4 Tickers pulled:
  CL=F  WTI Crude Oil Futures       (USD/barrel)
  BZ=F  Brent Crude Oil Futures     (USD/barrel)
  XLE   Energy Select Sector ETF    (USD/share)
  UGA   US Gasoline Fund ETF        (USD/share - tracks gas futures)

WHY THESE:
  CL=F + BZ=F = the 2 global crude oil benchmarks.
                Crude is the #1 input cost for gasoline.
  XLE         = tracks ExxonMobil, Chevron etc.
                Shows energy sector health.
  UGA         = directly tracks gasoline futures prices.
                Best real-time proxy for pump prices.

COST: Completely FREE. No API key. No signup.
      yfinance is already installed in docker-compose.

SCHEDULE: Daily 8:00 AM UTC
================================================
"""

from airflow import DAG
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

from datetime import datetime, timedelta

default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


def get_cursor():
    hook = SnowflakeHook(snowflake_conn_id="snowflake_conn")
    conn = hook.get_conn()
    return conn.cursor()


# ============================================================
# EXTRACT - Yahoo Finance (yfinance)
# ============================================================
@task
def extract_energy_market():
    """
    Pulls 4 years of weekly OHLCV data for 4 energy tickers
    from Yahoo Finance using the yfinance library.
    No API key needed.

    Returns list of dicts - one per (ticker, week).
    """
    import yfinance as yf

    tickers = {
        "CL=F": "WTI Crude Oil Futures",
        "BZ=F": "Brent Crude Oil Futures",
        "XLE":  "Energy Select Sector ETF",
        "UGA":  "US Gasoline Fund ETF",
    }

    all_records = []

    for ticker_symbol, ticker_name in tickers.items():
        try:
            ticker = yf.Ticker(ticker_symbol)
            df = ticker.history(period="4y", interval="1wk")
            df = df.reset_index()

            count = 0
            for _, row in df.iterrows():
                week_date = row["Date"]
                if hasattr(week_date, "date"):
                    week_date = week_date.date()

                # Skip rows where close price is NaN
                if row["Close"] != row["Close"]:
                    continue

                all_records.append({
                    "week_date":   str(week_date),
                    "ticker":      ticker_symbol,
                    "ticker_name": ticker_name,
                    "open":   round(float(row["Open"]),  4) if row["Open"]  == row["Open"]  else None,
                    "high":   round(float(row["High"]),  4) if row["High"]  == row["High"]  else None,
                    "low":    round(float(row["Low"]),   4) if row["Low"]   == row["Low"]   else None,
                    "close":  round(float(row["Close"]), 4),
                    "volume": int(row["Volume"]) if row["Volume"] == row["Volume"] else None,
                })
                count += 1

            print(f"  {ticker_symbol} ({ticker_name}): {count} records")

        except Exception as e:
            print(f"  WARNING: Failed to fetch {ticker_symbol}: {e}")

    print(f"Total energy market records extracted: {len(all_records)}")
    return all_records


# ============================================================
# TRANSFORM - Energy Market
# ============================================================
@task
def transform_energy_market(raw_records: list):
    """
    Converts list of dicts to list of tuples
    aligned with RAW.ENERGY_MARKET_PRICES schema.
    """
    records = []
    for r in raw_records:
        records.append((
            r["week_date"],
            r["ticker"],
            r["ticker_name"],
            r["open"],
            r["high"],
            r["low"],
            r["close"],
            r["volume"],
        ))
    print(f"Transformed {len(records)} energy market records")
    return records


# ============================================================
# LOAD - RAW.ENERGY_MARKET_PRICES
# ============================================================
@task
def load_energy_market(records: list):
    """
    MERGE upsert into RAW.ENERGY_MARKET_PRICES.
    Key: WEEK_DATE + TICKER
    Wrapped in SQL transaction with try/except/rollback.
    """
    cur = get_cursor()

    try:
        cur.execute("USE DATABASE USER_DB_FERRET")
        cur.execute("USE SCHEMA RAW")
        cur.execute("BEGIN")

        cur.execute("""
            CREATE TEMP TABLE ENERGY_MARKET_STAGE (
                WEEK_DATE   DATE,
                TICKER      STRING,
                TICKER_NAME STRING,
                OPEN_PRICE  FLOAT,
                HIGH_PRICE  FLOAT,
                LOW_PRICE   FLOAT,
                CLOSE_PRICE FLOAT,
                VOLUME      BIGINT
            )
        """)

        cur.executemany("""
            INSERT INTO ENERGY_MARKET_STAGE (
                WEEK_DATE, TICKER, TICKER_NAME,
                OPEN_PRICE, HIGH_PRICE, LOW_PRICE, CLOSE_PRICE, VOLUME
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """, records)

        cur.execute("""
            MERGE INTO RAW.ENERGY_MARKET_PRICES t
            USING ENERGY_MARKET_STAGE s
            ON t.WEEK_DATE = s.WEEK_DATE AND t.TICKER = s.TICKER

            WHEN MATCHED THEN UPDATE SET
                TICKER_NAME = s.TICKER_NAME,
                OPEN_PRICE  = s.OPEN_PRICE,
                HIGH_PRICE  = s.HIGH_PRICE,
                LOW_PRICE   = s.LOW_PRICE,
                CLOSE_PRICE = s.CLOSE_PRICE,
                VOLUME      = s.VOLUME,
                LOAD_TS     = CURRENT_TIMESTAMP()

            WHEN NOT MATCHED THEN INSERT (
                WEEK_DATE, TICKER, TICKER_NAME,
                OPEN_PRICE, HIGH_PRICE, LOW_PRICE, CLOSE_PRICE, VOLUME, LOAD_TS
            ) VALUES (
                s.WEEK_DATE, s.TICKER, s.TICKER_NAME,
                s.OPEN_PRICE, s.HIGH_PRICE, s.LOW_PRICE,
                s.CLOSE_PRICE, s.VOLUME, CURRENT_TIMESTAMP()
            )
        """)

        cur.execute("COMMIT")
        print(f"RAW.ENERGY_MARKET_PRICES UPSERT SUCCESS - {len(records)} records")

    except Exception as e:
        cur.execute("ROLLBACK")
        raise e


# ============================================================
# DAG DEFINITION
# ============================================================
with DAG(
    dag_id="FuelPrice_Realtime_ETL",
    start_date=datetime(2026, 5, 1),
    schedule="0 8 * * *",   # Daily 8:00 AM UTC
    catchup=False,
    tags=["ETL", "Fuel", "yfinance", "Realtime", "Snowflake"],
    default_args=default_args,
) as dag:

    raw     = extract_energy_market()
    t       = transform_energy_market(raw)
    load_energy_market(t)
