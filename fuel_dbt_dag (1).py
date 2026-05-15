"""
fuel_dbt_dag.py  — DAG 4 of 4
================================================
PURPOSE:
  Runs dbt run → dbt test → dbt snapshot
  using Snowflake credentials from Airflow Connection.
  No hardcoded passwords anywhere.

SCHEDULE: Daily 10:00 AM UTC
  (runs after FuelPrice_TrainPredict at 9:00 AM)

REQUIRES:
  - dbt project files mounted at /opt/airflow/dbt
  - snowflake_conn Airflow Connection
================================================
"""


from pendulum import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

DBT_PROJECT_DIR = "/opt/airflow/dbt"

dbt_env = {
    "DBT_TYPE": "snowflake",
    "DBT_ACCOUNT": "{{ conn.snowflake_conn.extra_dejson.account }}",
    "DBT_USER": "{{ conn.snowflake_conn.login }}",
    "DBT_PASSWORD": "{{ conn.snowflake_conn.password }}",
    "DBT_DATABASE": "{{ conn.snowflake_conn.extra_dejson.database }}",
    "DBT_SCHEMA": "DBT",
    "DBT_WAREHOUSE": "{{ conn.snowflake_conn.extra_dejson.warehouse }}",
    "DBT_ROLE": "{{ conn.snowflake_conn.extra_dejson.role }}",
}

with DAG(
    dag_id="FuelPrice_DBT",
    start_date=datetime(2026, 5, 1),
    description="Runs dbt models, tests, and snapshots for fuel price analytics ELT",
    schedule="0 10 * * *",
    catchup=False,
    tags=["DBT", "ELT", "Fuel", "Snowflake"],
) as dag:

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=(
            f"dbt run "
            f"--profiles-dir {DBT_PROJECT_DIR} "
            f"--project-dir {DBT_PROJECT_DIR}"
        ),
        env=dbt_env,
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=(
            f"dbt test "
            f"--profiles-dir {DBT_PROJECT_DIR} "
            f"--project-dir {DBT_PROJECT_DIR}"
        ),
        env=dbt_env,
    )

    dbt_snapshot = BashOperator(
        task_id="dbt_snapshot",
        bash_command=(
            f"dbt snapshot "
            f"--profiles-dir {DBT_PROJECT_DIR} "
            f"--project-dir {DBT_PROJECT_DIR}"
        ),
        env=dbt_env,
    )

    dbt_run >> dbt_test >> dbt_snapshot
