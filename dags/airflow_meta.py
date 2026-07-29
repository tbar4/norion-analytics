"""Airflow's own metadata -> Postgres schema `raw`, then its dbt models.

Makes the platform observable from the warehouse: run outcomes, task durations,
retries and failures, plus rows moved per dlt load. Feeds the `dag_runs`,
`task_runs` and `pipeline_loads` marts and their Cube models.

Two Connections, two different Postgres instances — see reference/platform.md,
where confusing them is called out as the easiest serious mistake here:

    airflow-db            SOURCE, the Airflow metadata database, port 5433
    norion-analytics-pg   DESTINATION, the warehouse, port 5432

This DAG reads its own metadata while running, so its own dag_run row is
in-flight and lands with state `running`. That is expected; the next run
corrects it, because the pipeline merges on `updated_at`.
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

WAREHOUSE_CONN_ID = "norion-analytics-pg"
AIRFLOW_DB_CONN_ID = "airflow-db"

# The dbt tag this source owns, set on the staging folder in dbt_project.yml.
SOURCE_TAG = "airflow_meta"

DBT_PROJECT_DIR = Path("/opt/airflow/include/dbt_projects/warehouse")

profile_config = ProfileConfig(
    profile_name="warehouse",
    target_name="dev",
    profile_mapping=PostgresUserPasswordProfileMapping(
        conn_id=WAREHOUSE_CONN_ID,
        profile_args={"schema": "analytics"},
    ),
)

execution_config = ExecutionConfig(dbt_executable_path="/opt/dbt-venv/bin/dbt")

# Builds only this source's slice of the dbt graph. SUBPROCESS is required:
# the DBT_RUNNER default needs dbt importable from the Airflow environment, and
# dbt is isolated in /opt/dbt-venv. See reference/platform.md.
render_config = RenderConfig(
    select=[f"tag:{SOURCE_TAG}+"],
    invocation_mode=InvocationMode.SUBPROCESS,
    dbt_executable_path="/opt/dbt-venv/bin/dbt",
)


@dag(
    dag_id="airflow_meta",
    # Hourly. Unlike the ingestion DAGs this has no external API to be polite
    # to — the source is a Postgres instance on the same host — so it can run
    # often enough to make a monitoring dashboard feel current.
    # At :40, clear of the DAG runs on the hour and the deploy check at :20.
    schedule="40 * * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["dlt", "observability", "raw"],
    doc_md=__doc__,
)
def airflow_meta():
    @task(retries=3, retry_delay=timedelta(minutes=10))
    def load() -> str:
        # Imported inside the task so a broken pipeline module cannot stop the
        # whole DAG file from parsing.
        from airflow.hooks.base import BaseHook

        from pipelines.airflow_meta_pipeline import load_airflow_meta

        # `conn.schema` is the DATABASE name — Airflow's naming, not ours. It
        # must be `airflow` here; it was originally `public`, which is a
        # Postgres schema and not a database, and failed with
        # `FATAL: database "public" does not exist`.
        src = BaseHook.get_connection(AIRFLOW_DB_CONN_ID)
        source_url = (
            f"postgresql+psycopg2://{src.login}:{src.password}"
            f"@{src.host}:{src.port or 5432}/{src.schema}"
        )

        dest = BaseHook.get_connection(WAREHOUSE_CONN_ID)
        credentials = {
            "database": dest.schema,
            "username": dest.login,
            "password": dest.password,
            "host": dest.host,
            "port": dest.port or 5432,
        }

        return load_airflow_meta(
            source_credentials=source_url,
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


airflow_meta()
