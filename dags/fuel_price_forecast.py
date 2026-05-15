"""
fuel_price_forecast.py  - DAG 3 of 4
================================================
PURPOSE:
  Trains a Snowflake ML Forecast model on RAW.FUEL_PRICES
  and generates a 12-week forward price prediction.

INPUT:  RAW.FUEL_PRICES (populated by FuelPrice_EIA_ETL)
OUTPUT:
  ADHOC.FUEL_PRICE_TRAIN_VIEW     - clean training view
  ADHOC.FUEL_PRICE_FORECAST       - raw ML forecast output
  ANALYTICS.FUEL_PRICE_FINAL      - historical + forecast UNION ALL
  ANALYTICS.FUEL_PRICE_MODEL_METRICS - evaluation metrics

SCHEDULE: Daily 9:00 AM UTC
  (runs 1 hour after FuelPrice_EIA_ETL at 8:00 AM)
================================================
"""

from __future__ import annotations

from datetime import datetime, timedelta
from airflow import DAG
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


def get_conn_and_cursor():
    hook = SnowflakeHook(snowflake_conn_id="snowflake_conn")
    conn = hook.get_conn()
    return conn, conn.cursor()


with DAG(
    dag_id="FuelPrice_TrainPredict",
    start_date=datetime(2026, 5, 1),
    schedule="0 9 * * *",   # Daily 9:00 AM UTC
    catchup=False,
    default_args=default_args,
    tags=["ML", "Forecast", "Fuel", "Snowflake"],
) as dag:

    # Table names
    train_input  = "USER_DB_FERRET.RAW.FUEL_PRICES"
    train_view   = "USER_DB_FERRET.ADHOC.FUEL_PRICE_TRAIN_VIEW"
    model_name   = "USER_DB_FERRET.ANALYTICS.FUEL_PRICE_FORECAST_MODEL"
    forecast_tbl = "USER_DB_FERRET.ADHOC.FUEL_PRICE_FORECAST"
    final_tbl    = "USER_DB_FERRET.ANALYTICS.FUEL_PRICE_FINAL"
    metrics_tbl  = "USER_DB_FERRET.ANALYTICS.FUEL_PRICE_MODEL_METRICS"

    # ============================================================
    # TASK 1 - Create training view + train model
    # ============================================================
    @task
    def train():
        """
        1. Creates a clean training view with 3 columns:
              REGION, WEEK_DATE, REGULAR_GASOLINE_PRICE
           (same pattern as Lab 1/2: CITY, DATE, TEMP_MAX)

        2. Trains SNOWFLAKE.ML.FORECAST on that view.
           REGION is the series column (like CITY in lab).
           ON_ERROR: SKIP means one bad series won't block others.

        3. Appends evaluation metrics to metrics table.
        """
        conn, cur = get_conn_and_cursor()
        try:
            cur.execute("USE DATABASE USER_DB_FERRET")
            cur.execute("BEGIN")

            # Step 1: Create clean training view
            cur.execute(f"""
                CREATE OR REPLACE VIEW {train_view} AS
                SELECT
                    REGION,
                    WEEK_DATE,
                    REGULAR_GASOLINE_PRICE
                FROM {train_input}
                WHERE REGULAR_GASOLINE_PRICE IS NOT NULL
                  AND REGION = 'US_NATIONAL'
                ORDER BY WEEK_DATE
            """)
            print("Training view created")

            # Step 2: Train Snowflake ML Forecast model
            cur.execute(f"""
                CREATE OR REPLACE SNOWFLAKE.ML.FORECAST {model_name}
                (
                    INPUT_DATA        => SYSTEM$REFERENCE('VIEW', '{train_view}'),
                    SERIES_COLNAME    => 'REGION',
                    TIMESTAMP_COLNAME => 'WEEK_DATE',
                    TARGET_COLNAME    => 'REGULAR_GASOLINE_PRICE',
                    CONFIG_OBJECT     => {{'ON_ERROR': 'SKIP'}}
                )
            """)
            print("ML Forecast model trained")

            # Step 3: Store evaluation metrics
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {metrics_tbl}
                AS
                SELECT CURRENT_TIMESTAMP() AS RUN_TS, *
                FROM TABLE({model_name}!SHOW_EVALUATION_METRICS())
                LIMIT 0
            """)

            cur.execute(f"""
                INSERT INTO {metrics_tbl}
                SELECT CURRENT_TIMESTAMP() AS RUN_TS, *
                FROM TABLE({model_name}!SHOW_EVALUATION_METRICS())
            """)
            print("Evaluation metrics stored")

            cur.execute("COMMIT")

        except Exception as e:
            cur.execute("ROLLBACK")
            raise
        finally:
            cur.close()
            conn.close()

    # ============================================================
    # TASK 2 - Generate 12-week forecast + build final table
    # ============================================================
    @task
    def predict():
        """
        1. Calls model!FORECAST for 12 weeks ahead.
        2. Captures output using RESULT_SCAN(LAST_QUERY_ID()).
           (This is required - Snowflake ML output cannot be
            written directly to a table any other way.)
        3. Creates FUEL_PRICE_FINAL as UNION ALL of:
              historical actuals  (ACTUAL filled, FORECAST null)
              forecasted values   (FORECAST filled, ACTUAL null)
           Same pattern as Lab 1/2 CITY_WEATHER_FINAL.
        """
        conn, cur = get_conn_and_cursor()
        try:
            cur.execute("USE DATABASE USER_DB_FERRET")

            # Step 1: Run forecast
            cur.execute(f"""
                CALL {model_name}!FORECAST(
                    FORECASTING_PERIODS => 12,
                    CONFIG_OBJECT => {{'prediction_interval': 0.95}}
                )
            """)

            # Step 2: Capture query ID immediately
            cur.execute("SELECT LAST_QUERY_ID()")
            query_id = cur.fetchone()[0]

            # Step 3: Store raw forecast results
            cur.execute(f"""
                CREATE OR REPLACE TABLE {forecast_tbl} AS
                SELECT *
                FROM TABLE(RESULT_SCAN('{query_id}'))
            """)
            print("Forecast results stored")

            # Step 4: Build unified final table (historical + forecast)
            cur.execute(f"""
                CREATE OR REPLACE TABLE {final_tbl} AS

                -- Historical actuals
                SELECT
                    REGION,
                    WEEK_DATE,
                    REGULAR_GASOLINE_PRICE AS ACTUAL,
                    NULL                   AS FORECAST,
                    NULL                   AS LOWER_BOUND,
                    NULL                   AS UPPER_BOUND,
                    'HISTORICAL'           AS RECORD_TYPE
                FROM {train_input}
                WHERE REGION = 'US_NATIONAL'

                UNION ALL

                -- Forecasted values (12 weeks ahead)
                SELECT
                    REPLACE(SERIES, '"', '') AS REGION,
                    TS                       AS WEEK_DATE,
                    NULL                     AS ACTUAL,
                    FORECAST,
                    LOWER_BOUND,
                    UPPER_BOUND,
                    'FORECAST'               AS RECORD_TYPE
                FROM {forecast_tbl}
            """)
            print("FUEL_PRICE_FINAL created successfully")

        except Exception as e:
            raise
        finally:
            cur.close()
            conn.close()

    # ============================================================
    # DAG ORDER: train first, then predict
    # ============================================================
    train_task   = train()
    predict_task = predict()
    train_task >> predict_task
