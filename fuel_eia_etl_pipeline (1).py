"""
fuel_eia_etl_pipeline.py — DAG 1

Purpose:
  Pulls real weekly EIA fuel price data and loads it into Snowflake.

Loads:
  USER_DB_FERRET.RAW.FUEL_PRICES
  USER_DB_FERRET.RAW.REGIONAL_FUEL_PRICES

Important:
  Uses EIA series IDs because these return the correct gasoline values.
"""

from datetime import datetime, timedelta

import requests
from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook


DATABASE_NAME = "USER_DB_FERRET"


default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
}


def get_cursor():
    hook = SnowflakeHook(snowflake_conn_id="snowflake_conn")
    conn = hook.get_conn()
    return conn, conn.cursor()


# ============================================================
# EXTRACT — EIA National Prices
# ============================================================
@task
def extract_national_prices():
    """
    Pulls weekly U.S. national fuel prices from EIA API v2.

    Correct EIA weekly series:
      EMM_EPMR_PTE_NUS_DPG = Regular gasoline
      EMM_EPMM_PTE_NUS_DPG = Midgrade gasoline
      EMM_EPMP_PTE_NUS_DPG = Premium gasoline
      EMD_EPD2D_PTE_NUS_DPG = Diesel
    """
    api_key = Variable.get("eia_api_key")
    base_url = "https://api.eia.gov/v2/petroleum/pri/gnd/data/"

    series = {
        "regular_gasoline": "EMM_EPMR_PTE_NUS_DPG",
        "midgrade_gasoline": "EMM_EPMM_PTE_NUS_DPG",
        "premium_gasoline": "EMM_EPMP_PTE_NUS_DPG",
        "diesel": "EMD_EPD2D_PTE_NUS_DPG",
    }

    results = {}

    for label, series_id in series.items():
        params = {
            "api_key": api_key,
            "frequency": "weekly",
            "data[0]": "value",
            "facets[series][]": series_id,
            "sort[0][column]": "period",
            "sort[0][direction]": "desc",
            "length": 200,
            "offset": 0,
        }

        response = requests.get(base_url, params=params, timeout=30)

        if response.status_code != 200:
            raise RuntimeError(
                f"EIA API failed for {label}: "
                f"{response.status_code} | {response.text}"
            )

        rows = response.json().get("response", {}).get("data", [])
        results[label] = rows
        print(f"{label}: {len(rows)} records")

    return results


# ============================================================
# EXTRACT — EIA Regional Prices
# ============================================================
@task
def extract_regional_prices():
    """
    Pulls weekly regular gasoline prices by PADD region from EIA API v2.
    """
    api_key = Variable.get("eia_api_key")
    base_url = "https://api.eia.gov/v2/petroleum/pri/gnd/data/"

    region_series = {
        "EAST_COAST": "EMM_EPMR_PTE_R10_DPG",
        "MIDWEST": "EMM_EPMR_PTE_R20_DPG",
        "GULF_COAST": "EMM_EPMR_PTE_R30_DPG",
        "ROCKY_MOUNTAIN": "EMM_EPMR_PTE_R40_DPG",
        "WEST_COAST": "EMM_EPMR_PTE_R50_DPG",
        "CALIFORNIA": "EMM_EPMR_PTE_SCA_DPG",
    }

    results = {}

    for region, series_id in region_series.items():
        params = {
            "api_key": api_key,
            "frequency": "weekly",
            "data[0]": "value",
            "facets[series][]": series_id,
            "sort[0][column]": "period",
            "sort[0][direction]": "desc",
            "length": 104,
            "offset": 0,
        }

        response = requests.get(base_url, params=params, timeout=30)

        if response.status_code == 200:
            rows = response.json().get("response", {}).get("data", [])
            results[region] = rows
            print(f"{region}: {len(rows)} records")
        else:
            print(
                f"WARNING: EIA regional failed for {region}: "
                f"{response.status_code} | {response.text}"
            )
            results[region] = []

    return results


# ============================================================
# TRANSFORM — National Prices
# ============================================================
@task
def transform_national(raw_data: dict):
    """
    Combines the four EIA national fuel series into one row per week.
    """
    weekly = {}

    column_map = {
        "regular_gasoline": "REGULAR_GASOLINE",
        "midgrade_gasoline": "MIDGRADE_GASOLINE",
        "premium_gasoline": "PREMIUM_GASOLINE",
        "diesel": "DIESEL",
    }

    for label, rows in raw_data.items():
        column_name = column_map[label]

        for row in rows:
            period = row.get("period")
            value = row.get("value")

            if not period or value is None:
                continue

            if period not in weekly:
                weekly[period] = {
                    "REGULAR_GASOLINE": None,
                    "MIDGRADE_GASOLINE": None,
                    "PREMIUM_GASOLINE": None,
                    "DIESEL": None,
                }

            try:
                weekly[period][column_name] = round(float(value), 4)
            except (ValueError, TypeError):
                pass

    records = []

    for period, prices in weekly.items():
        records.append(
            (
                period,
                "US_NATIONAL",
                prices["REGULAR_GASOLINE"],
                prices["MIDGRADE_GASOLINE"],
                prices["PREMIUM_GASOLINE"],
                prices["DIESEL"],
                "USD_PER_GALLON",
                "EIA_WEEKLY",
            )
        )

    print(f"Transformed {len(records)} national records")

    if records:
        print(f"Sample national record: {records[0]}")

    return records


# ============================================================
# TRANSFORM — Regional Prices
# ============================================================
@task
def transform_regional(raw_data: dict):
    """
    Flattens regional EIA data into rows for RAW.REGIONAL_FUEL_PRICES.
    """
    records = []

    for region, rows in raw_data.items():
        for row in rows:
            period = row.get("period")
            value = row.get("value")

            if not period or value is None:
                continue

            try:
                records.append(
                    (
                        period,
                        region,
                        round(float(value), 4),
                        "REGULAR_GASOLINE",
                        "USD_PER_GALLON",
                        "EIA_REGIONAL",
                    )
                )
            except (ValueError, TypeError):
                pass

    print(f"Transformed {len(records)} regional records")

    if records:
        print(f"Sample regional record: {records[0]}")

    return records


# ============================================================
# LOAD — National Prices
# ============================================================
@task
def load_national_prices(records: list):
    if not records:
        print("No national records to load")
        return

    conn, cur = get_cursor()

    try:
        cur.execute(f"USE DATABASE {DATABASE_NAME}")
        cur.execute("USE SCHEMA RAW")
        cur.execute("BEGIN")

        cur.execute(
            """
            CREATE TEMP TABLE FUEL_PRICES_STAGE (
                WEEK_DATE DATE,
                REGION STRING,
                REGULAR_GASOLINE_PRICE FLOAT,
                MIDGRADE_GASOLINE_PRICE FLOAT,
                PREMIUM_GASOLINE_PRICE FLOAT,
                DIESEL_PRICE FLOAT,
                PRICE_UNIT STRING,
                SOURCE STRING
            )
            """
        )

        cur.executemany(
            """
            INSERT INTO FUEL_PRICES_STAGE (
                WEEK_DATE,
                REGION,
                REGULAR_GASOLINE_PRICE,
                MIDGRADE_GASOLINE_PRICE,
                PREMIUM_GASOLINE_PRICE,
                DIESEL_PRICE,
                PRICE_UNIT,
                SOURCE
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """,
            records,
        )

        cur.execute(
            """
            MERGE INTO RAW.FUEL_PRICES t
            USING FUEL_PRICES_STAGE s
            ON t.WEEK_DATE = s.WEEK_DATE
            AND t.REGION = s.REGION

            WHEN MATCHED THEN UPDATE SET
                REGULAR_GASOLINE_PRICE = s.REGULAR_GASOLINE_PRICE,
                MIDGRADE_GASOLINE_PRICE = s.MIDGRADE_GASOLINE_PRICE,
                PREMIUM_GASOLINE_PRICE = s.PREMIUM_GASOLINE_PRICE,
                DIESEL_PRICE = s.DIESEL_PRICE,
                PRICE_UNIT = s.PRICE_UNIT,
                SOURCE = s.SOURCE,
                LOAD_TS = CURRENT_TIMESTAMP()

            WHEN NOT MATCHED THEN INSERT (
                WEEK_DATE,
                REGION,
                REGULAR_GASOLINE_PRICE,
                MIDGRADE_GASOLINE_PRICE,
                PREMIUM_GASOLINE_PRICE,
                DIESEL_PRICE,
                PRICE_UNIT,
                SOURCE,
                LOAD_TS
            )
            VALUES (
                s.WEEK_DATE,
                s.REGION,
                s.REGULAR_GASOLINE_PRICE,
                s.MIDGRADE_GASOLINE_PRICE,
                s.PREMIUM_GASOLINE_PRICE,
                s.DIESEL_PRICE,
                s.PRICE_UNIT,
                s.SOURCE,
                CURRENT_TIMESTAMP()
            )
            """
        )

        cur.execute("COMMIT")
        print(f"RAW.FUEL_PRICES UPSERT SUCCESS — {len(records)} records")

    except Exception:
        cur.execute("ROLLBACK")
        raise

    finally:
        cur.close()
        conn.close()


# ============================================================
# LOAD — Regional Prices
# ============================================================
@task
def load_regional_prices(records: list):
    if not records:
        print("No regional records to load")
        return

    conn, cur = get_cursor()

    try:
        cur.execute(f"USE DATABASE {DATABASE_NAME}")
        cur.execute("USE SCHEMA RAW")
        cur.execute("BEGIN")

        cur.execute(
            """
            CREATE TEMP TABLE REGIONAL_STAGE (
                WEEK_DATE DATE,
                REGION STRING,
                PRICE FLOAT,
                FUEL_TYPE STRING,
                PRICE_UNIT STRING,
                SOURCE STRING
            )
            """
        )

        cur.executemany(
            """
            INSERT INTO REGIONAL_STAGE (
                WEEK_DATE,
                REGION,
                PRICE,
                FUEL_TYPE,
                PRICE_UNIT,
                SOURCE
            )
            VALUES (%s, %s, %s, %s, %s, %s)
            """,
            records,
        )

        cur.execute(
            """
            MERGE INTO RAW.REGIONAL_FUEL_PRICES t
            USING REGIONAL_STAGE s
            ON t.WEEK_DATE = s.WEEK_DATE
            AND t.REGION = s.REGION
            AND t.FUEL_TYPE = s.FUEL_TYPE

            WHEN MATCHED THEN UPDATE SET
                PRICE = s.PRICE,
                PRICE_UNIT = s.PRICE_UNIT,
                SOURCE = s.SOURCE,
                LOAD_TS = CURRENT_TIMESTAMP()

            WHEN NOT MATCHED THEN INSERT (
                WEEK_DATE,
                REGION,
                PRICE,
                FUEL_TYPE,
                PRICE_UNIT,
                SOURCE,
                LOAD_TS
            )
            VALUES (
                s.WEEK_DATE,
                s.REGION,
                s.PRICE,
                s.FUEL_TYPE,
                s.PRICE_UNIT,
                s.SOURCE,
                CURRENT_TIMESTAMP()
            )
            """
        )

        cur.execute("COMMIT")
        print(f"RAW.REGIONAL_FUEL_PRICES UPSERT SUCCESS — {len(records)} records")

    except Exception:
        cur.execute("ROLLBACK")
        raise

    finally:
        cur.close()
        conn.close()


# ============================================================
# DAG DEFINITION
# ============================================================
with DAG(
    dag_id="FuelPrice_EIA_ETL",
    start_date=datetime(2026, 5, 1),
    schedule="0 6 * * 3",
    catchup=False,
    tags=["ETL", "Fuel", "EIA", "Snowflake"],
    default_args=default_args,
) as dag:

    raw_national = extract_national_prices()
    national_records = transform_national(raw_national)
    load_national_prices(national_records)

    raw_regional = extract_regional_prices()
    regional_records = transform_regional(raw_regional)
    load_regional_prices(regional_records)
