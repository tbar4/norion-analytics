"""NASA DONKI space weather -> Postgres schema `raw`, then its dbt models.

Loads all eleven DONKI component services — CME, CMEAnalysis, GST, IPS, FLR,
SEP, MPC, RBE, HSS, WSAEnlilSimulations and notifications — as resources of a
single dlt source. One DAG, one dbt tag, one state directory.

Credentials live in Airflow, not in .dlt/secrets.toml: the `NASA_API_KEY`
Variable and the `norion-analytics-pg` Connection. This DAG reads both and
hands them to the pipeline explicitly, so the pipeline module itself stays free
of Airflow imports and remains runnable on the workstation.

`NASA_API_KEY` is an Airflow **Variable**, stored in the metadata database. It
is not an environment variable and is not in .env — `Variable.get` is the only
way to reach it.

The Connection points at 10.0.0.50 — the host's LAN address — rather than a
docker network alias. That is what lets the scheduler reach postgres_db, which
sits on the separate `postgres_default` bridge network.
"""

from __future__ import annotations

from datetime import datetime
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
SOURCE_TAG = "nasa_donki"

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

# dbt lives in its own virtualenv, so Cosmos is told where the binary is rather
# than importing dbt from the Airflow environment.
execution_config = ExecutionConfig(dbt_executable_path="/opt/dbt-venv/bin/dbt")

# This DAG builds only its own slice of the dbt graph, not the whole project.
# `tag:nasa_donki` selects the staging models tagged by the folder block in
# dbt_project.yml; the trailing `+` pulls in everything downstream of them.
#
# SUBPROCESS is required, not a preference. RenderConfig.invocation_mode
# defaults to DBT_RUNNER, which runs dbt in-process and so needs dbt importable
# from the Airflow environment. It is not — dbt is isolated in /opt/dbt-venv.
# Cosmos does not detect that cleanly (its check is find_spec("dbt"), which
# returns None rather than raising), so parsing dies on a message about
# dbt_executable_path instead. Deleting that path is the wrong response.
render_config = RenderConfig(
    select=[f"tag:{SOURCE_TAG}+"],
    invocation_mode=InvocationMode.SUBPROCESS,
    dbt_executable_path="/opt/dbt-venv/bin/dbt",
)

# A trailing year on every run. The pipeline merges on each event's natural
# key, so a wide window is not wasteful the way it would be under `replace` —
# it re-reads recent history and overwrites any event NASA has revised, while
# older events already loaded simply stay put. Volumes are small: roughly 1,200
# CMEs and 700 flares a year.
DAYS_BACK = 365


@dag(
    dag_id="nasa_donki",
    # 07:00, an hour after nasa_apod, so the two do not hit api.nasa.gov with
    # the same key at the same moment.
    schedule="0 7 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    # One run at a time. Two concurrent runs share the same dlt state directory
    # and make eleven duplicate API calls apiece.
    max_active_runs=1,
    tags=["dlt", "nasa", "space-weather", "raw"],
    doc_md=__doc__,
)
def nasa_donki():
    @task
    def load() -> str:
        # Imported inside the task so a broken pipeline module can't stop the
        # whole DAG file from parsing.
        from airflow.hooks.base import BaseHook
        from airflow.models import Variable

        from pipelines.nasa_donki_pipeline import load_donki

        conn = BaseHook.get_connection(POSTGRES_CONN_ID)
        credentials = {
            "database": conn.schema,
            "username": conn.login,
            "password": conn.password,
            "host": conn.host,
            "port": conn.port or 5432,
        }

        return load_donki(
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

    load() >> dbt_models


nasa_donki()
