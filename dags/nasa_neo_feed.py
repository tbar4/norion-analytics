"""NASA NeoWs feed -> Postgres schema `raw`, then this source's dbt models.

Asteroid close approaches to Earth. One row per asteroid per approach date.

Credentials live in Airflow, not in .dlt/secrets.toml: the `NASA_API_KEY`
Variable (shared with nasa_apod — same api.nasa.gov key) and the
`norion-analytics-pg` Connection.

Runs at 07:00, an hour after nasa_apod, so the two NASA sources do not contend
for the same rate limit. The pipeline pulls a trailing 7-day window while the
schedule is daily, giving 7x overlap — a missed or failed run is picked up by
the next six without leaving a gap. That tolerance is the reason the window is
not narrowed to match the schedule.
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
API_KEY_VAR = "NASA_API_KEY"

# The dbt tag this source owns, set on the staging folder in dbt_project.yml.
SOURCE_TAG = "nasa_neo_feed"

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
    dag_id="nasa_neo_feed",
    schedule="0 7 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    # One run at a time. Concurrent runs share the same dlt state directory and
    # hammer the same API.
    max_active_runs=1,
    tags=["dlt", "nasa_neo_feed", "raw"],
    doc_md=__doc__,
)
def nasa_neo_feed():
    # Retry at the orchestrator layer. api.nasa.gov returns intermittent 500s,
    # and dlt's in-request retry is not enough: nasa_apod exhausted its attempts
    # after 2m52s on 2026-07-29 and still failed, because the outage lasted
    # minutes. dlt handles blips; Airflow handles "the upstream was down".
    @task(retries=3, retry_delay=timedelta(minutes=10))
    def load() -> str:
        # Imported inside the task so a broken pipeline module cannot stop the
        # whole DAG file from parsing.
        from airflow.hooks.base import BaseHook
        from airflow.models import Variable

        from pipelines.nasa_neo_feed_pipeline import load_nasa_neo_feed

        conn = BaseHook.get_connection(POSTGRES_CONN_ID)
        credentials = {
            # Airflow's `schema` field is the DATABASE name, not a Postgres
            # schema. That naming trap is Airflow's, not ours.
            "database": conn.schema,
            "username": conn.login,
            "password": conn.password,
            "host": conn.host,
            "port": conn.port or 5432,
        }

        return load_nasa_neo_feed(
            api_key=Variable.get(API_KEY_VAR),
            credentials=credentials,
        )

    dbt_models = DbtTaskGroup(
        group_id="dbt_warehouse",
        project_config=ProjectConfig(dbt_project_path=DBT_PROJECT_DIR),
        profile_config=profile_config,
        execution_config=execution_config,
        render_config=render_config,
    )

    load() >> dbt_models


nasa_neo_feed()
