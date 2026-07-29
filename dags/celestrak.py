"""CelesTrak GP element sets -> Postgres schema `raw`, then this source's dbt models.

CelesTrak needs no API key, so unlike the NASA sources there is no Variable to
read — only the `norion-analytics-pg` Connection for the warehouse.

Schedule is every 8 hours rather than hourly on purpose. CelesTrak's usage
policy asks callers not to poll faster than the data actually changes, and GP
element sets are refreshed a few times a day. Three runs a day stays inside
that and still gives the screening engine fresh elements.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path

from airflow.sdk import dag, task
from cosmos import (
    DbtTaskGroup,
    ExecutionConfig,
    InvocationMode,
    ProfileConfig,
    ProjectConfig,
    RenderConfig,
)
from cosmos.profiles import PostgresUserPasswordProfileMapping

POSTGRES_CONN_ID = "norion-analytics-pg"

# The dbt tag this source owns, set on the staging folder in dbt_project.yml.
SOURCE_TAG = "celestrak"

DBT_PROJECT_DIR = Path("/opt/airflow/include/dbt_projects/warehouse")

# Cosmos generates dbt's profile from the Airflow Connection, so the warehouse
# password is never written to profiles.yml.
profile_config = ProfileConfig(
    profile_name="warehouse",
    target_name="dev",
    profile_mapping=PostgresUserPasswordProfileMapping(
        conn_id=POSTGRES_CONN_ID,
        profile_args={"schema": "analytics"},
    ),
)

execution_config = ExecutionConfig(dbt_executable_path="/opt/dbt-venv/bin/dbt")

# Builds only this source's slice of the dbt graph. SUBPROCESS is required:
# the DBT_RUNNER default needs dbt in the Airflow environment, and dbt is
# isolated in /opt/dbt-venv. See reference/platform.md.
render_config = RenderConfig(
    select=[f"tag:{SOURCE_TAG}+"],
    invocation_mode=InvocationMode.SUBPROCESS,
    dbt_executable_path="/opt/dbt-venv/bin/dbt",
)


@dag(
    dag_id="celestrak",
    schedule="0 */8 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    # One run at a time. Concurrent runs share the same dlt state directory and
    # hammer the same public endpoint.
    max_active_runs=1,
    tags=["dlt", "celestrak", "raw", "conjunction"],
    doc_md=__doc__,
)
def celestrak():
    # Retry at the orchestrator layer. dlt retries 429s and 5xx inside a single
    # request, which is not enough when a public endpoint is down for minutes
    # rather than seconds — nasa_apod exhausted dlt's attempts after 2m52s and
    # still failed. See the decision note referenced in the recipe.
    @task(retries=3, retry_delay=timedelta(minutes=10))
    def load() -> str:
        # Imported inside the task so a broken pipeline module cannot stop the
        # whole DAG file from parsing.
        from airflow.hooks.base import BaseHook

        from pipelines.celestrak_pipeline import load_celestrak

        conn = BaseHook.get_connection(POSTGRES_CONN_ID)
        credentials = {
            # conn.schema is Airflow's field for the DATABASE name, not a
            # Postgres schema. That naming trap is Airflow's, not ours.
            "database": conn.schema,
            "username": conn.login,
            "password": conn.password,
            "host": conn.host,
            "port": conn.port or 5432,
        }

        return load_celestrak(credentials=credentials)

    dbt_models = DbtTaskGroup(
        group_id="dbt_warehouse",
        project_config=ProjectConfig(dbt_project_path=DBT_PROJECT_DIR),
        profile_config=profile_config,
        execution_config=execution_config,
        render_config=render_config,
    )

    load() >> dbt_models


celestrak()
