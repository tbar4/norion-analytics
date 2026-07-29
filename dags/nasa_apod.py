"""NASA Astronomy Picture of the Day -> Postgres schema `raw`.

Credentials live in Airflow, not in .dlt/secrets.toml: the `NASA_API_KEY`
Variable and the `norion-analytics-pg` Connection. This DAG reads both and
hands them to the pipeline explicitly, so the pipeline module itself stays
free of Airflow imports and remains runnable on the workstation.

The Connection points at 10.0.0.50 — the host's LAN address — rather than a
docker network alias. That is what lets the scheduler reach postgres_db,
which sits on the separate `postgres_default` bridge network.
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
NASA_API_KEY_VAR = "NASA_API_KEY"

# The dbt tag this source owns, set on the staging folder in dbt_project.yml.
SOURCE_TAG = "nasa_apod"

DBT_PROJECT_DIR = Path("/opt/airflow/include/dbt_projects/warehouse")

# Cosmos generates dbt's profile from the Airflow Connection, so the warehouse
# password is never written to profiles.yml. That file is only for running dbt
# by hand.
profile_config = ProfileConfig(
    profile_name="warehouse",
    target_name="dev",
    profile_mapping=PostgresUserPasswordProfileMapping(
        conn_id=POSTGRES_CONN_ID,
        profile_args={"schema": "analytics"},
    ),
)

# dbt lives in its own virtualenv, so Cosmos is told where the binary is
# rather than importing dbt from the Airflow environment.
execution_config = ExecutionConfig(dbt_executable_path="/opt/dbt-venv/bin/dbt")

# This DAG builds only its own slice of the dbt graph, not the whole project.
# `tag:nasa_apod` selects the staging models tagged by the folder block in
# dbt_project.yml; the trailing `+` pulls in everything downstream of them, so
# marts fed by this source are rebuilt too. Without the selector, every ingest
# DAG would rebuild every model in the project on every run.
#
# Cosmos resolves the selector by running `dbt ls` at DAG-parse time, so it
# needs its own copy of the binary path — RenderConfig does not inherit it from
# ExecutionConfig.
#
# SUBPROCESS is required, not a preference. RenderConfig.invocation_mode
# defaults to DBT_RUNNER, which runs dbt in-process and so needs dbt importable
# from the Airflow environment. It is not — dbt is isolated in /opt/dbt-venv.
#
# Cosmos does not detect that cleanly: its check is find_spec("dbt"), which
# returns None rather than raising, so it concludes dbt IS in this environment
# and lets DBT_RUNNER stand. Parsing then dies on a message about the wrong
# thing entirely:
#
#   RenderConfig.dbt_executable_path is set, but it is not the same as the
#   system dbt executable path.
#
# Deleting dbt_executable_path is the wrong response to that. Setting
# SUBPROCESS is the right one.
render_config = RenderConfig(
    select=[f"tag:{SOURCE_TAG}+"],
    invocation_mode=InvocationMode.SUBPROCESS,
    dbt_executable_path="/opt/dbt-venv/bin/dbt",
)

# APOD publishes once a day. A trailing window on every run means a late or
# corrected entry gets picked up rather than being missed permanently.
DAYS_BACK = 365


@dag(
    dag_id="nasa_apod",
    schedule="0 6 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    # One run at a time. Two concurrent runs share the same dlt state directory
    # and hammer the same API — a manual trigger landing on top of a scheduled
    # run made NASA's API return 500 on 2026-07-29.
    max_active_runs=1,
    tags=["dlt", "nasa", "raw"],
    doc_md=__doc__,
)
def nasa_apod():
    # api.nasa.gov returns intermittent 500s on the full-year range. dlt already
    # retries inside the request (its default covers 429 and all 5xx) and that is
    # not enough: on 2026-07-29 the task retried for 2m52s and still failed,
    # because the outage lasted minutes rather than seconds.
    #
    # So the retry that matters is at the orchestrator layer — come back in ten
    # minutes, by which time NASA has recovered. dlt handles per-request blips;
    # Airflow handles "the upstream was down for a while".
    @task(retries=3, retry_delay=timedelta(minutes=10))
    def load() -> str:
        # Imported inside the task so a broken pipeline module can't stop the
        # whole DAG file from parsing.
        from airflow.hooks.base import BaseHook
        from airflow.models import Variable

        from pipelines.nasa_apod_pipeline import load_apod

        conn = BaseHook.get_connection(POSTGRES_CONN_ID)
        credentials = {
            "database": conn.schema,
            "username": conn.login,
            "password": conn.password,
            "host": conn.host,
            "port": conn.port or 5432,
        }

        return load_apod(
            api_key=Variable.get(NASA_API_KEY_VAR),
            credentials=credentials,
            days_back=DAYS_BACK,
        )

    dbt_models = DbtTaskGroup(
        group_id="dbt_warehouse",
        project_config=ProjectConfig(dbt_project_path=DBT_PROJECT_DIR),
        profile_config=profile_config,
        execution_config=execution_config,
        render_config=render_config,
    )

    # No pool here: the old 1-slot `duckdb` pool existed because DuckDB allows
    # a single writer. Postgres does not need it.
    load() >> dbt_models


nasa_apod()
