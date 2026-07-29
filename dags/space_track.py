"""Space-Track catalogue -> Postgres schema `raw`, then this source's dbt models.

Space-Track is the only source here that covers DEBRIS. Without it the
conjunction screen runs against active payloads only and knowingly
under-reports, so this DAG is what makes the screening universe complete.

Credentials live in Airflow, not in .dlt/secrets.toml: the `SPACE_TRACK_USER`
and `SPACE_TRACK_PASSWORD` Variables and the `norion-analytics-pg` Connection.

    airflow variables set SPACE_TRACK_USER '<identity>'
    airflow variables set SPACE_TRACK_PASSWORD '<password>'

SHIPPED PAUSED AND UNVERIFIED. As of 2026-07-29 those Variables had not been
set, so this pipeline has never run and its dbt models have never been built.
Set the Variables, unpause, and treat the first run as the real verification of
the column names in models/staging/space_track/.

Scheduled every 8 hours, which respects Space-Track's guidance of at most
hourly for GP and every 8 hours for CDM. The pipeline issues two bulk queries
per run against a documented ceiling of 30/minute and 300/hour, so it sits far
inside the limit — provided nobody rewrites it to query per object.
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
USER_VAR = "SPACE_TRACK_USER"
PASSWORD_VAR = "SPACE_TRACK_PASSWORD"

# The dbt tag this source owns, set on the staging folder in dbt_project.yml.
SOURCE_TAG = "space_track"

DBT_PROJECT_DIR = Path("/opt/airflow/include/dbt_projects/warehouse")

profile_config = ProfileConfig(
    profile_name="warehouse",
    target_name="dev",
    profile_mapping=PostgresUserPasswordProfileMapping(
        conn_id=POSTGRES_CONN_ID,
        profile_args={"schema": "analytics"},
    ),
)

execution_config = ExecutionConfig(dbt_executable_path="/opt/dbt-venv/bin/dbt")

render_config = RenderConfig(
    select=[f"tag:{SOURCE_TAG}+"],
    invocation_mode=InvocationMode.SUBPROCESS,
    dbt_executable_path="/opt/dbt-venv/bin/dbt",
)


@dag(
    dag_id="space_track",
    schedule="0 */8 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["dlt", "space_track", "raw", "conjunction"],
    doc_md=__doc__,
)
def space_track():
    # Retry at the orchestrator layer, as celestrak does. Space-Track is a
    # single upstream that is periodically down for maintenance windows longer
    # than any in-request retry will cover.
    @task(retries=3, retry_delay=timedelta(minutes=10), execution_timeout=timedelta(minutes=30))
    def load() -> str:
        # Imported inside the task so a broken pipeline module cannot stop the
        # whole DAG file from parsing.
        from airflow.hooks.base import BaseHook
        from airflow.models import Variable

        from pipelines.space_track_pipeline import load_space_track

        conn = BaseHook.get_connection(POSTGRES_CONN_ID)
        credentials = {
            # conn.schema is Airflow's field for the DATABASE name.
            "database": conn.schema,
            "username": conn.login,
            "password": conn.password,
            "host": conn.host,
            "port": conn.port or 5432,
        }

        return load_space_track(
            identity=Variable.get(USER_VAR),
            password=Variable.get(PASSWORD_VAR),
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


space_track()
