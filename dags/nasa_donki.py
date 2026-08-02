"""NASA DONKI space weather -> Postgres schema `raw`, then its dbt models.

Loads all eleven DONKI component services — CME, CMEAnalysis, GST, IPS, FLR,
SEP, MPC, RBE, HSS, WSAEnlilSimulations and notifications — as resources of a
single dlt source. One DAG, one dbt tag, one state directory.

A short window per run, ending at the run's own data interval rather than at
wall-clock now, so re-running an interval requests the same dates and merges
over its own previous output. Backfills go through the `start_date`/`end_date`
params, which the loader slices into 30-day requests.

This replaced a trailing 365-day window on 2026-08-02. That window was the
cause of this DAG's 503s: a year-wide DONKI/CME query takes 55 seconds upstream
and sits on the gateway's timeout, so it failed whenever NASA was under any
load. The query was valid — just too slow. See the pipeline module docstring
for measured timings and for why the window is a few days rather than one.

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
    # Backfill a range by hand. The loader slices it into 30-day requests, so
    # a multi-year range is safe to ask for:
    #   airflow dags trigger nasa_donki \
    #     -c '{"start_date": "2010-01-01", "end_date": "2020-12-31"}'
    params={"start_date": None, "end_date": None},
)
def nasa_donki():
    # api.nasa.gov returns intermittent 500s. dlt already retries inside the
    # request and that is not always enough — an outage lasting minutes
    # exhausts its attempts. This source makes eleven requests per run, so it
    # has eleven chances to hit one. Retry the whole task later instead.
    #
    # This is a backstop, not the main defence. The 503s that dominated this
    # DAG's history came from the 365-day window taking 55 seconds upstream and
    # sitting on the gateway timeout — a deterministic failure no retry
    # schedule could clear. The bounded window is the actual fix.
    @task(retries=3, retry_delay=timedelta(minutes=10))
    def load() -> str:
        # Imported inside the task so a broken pipeline module can't stop the
        # whole DAG file from parsing.
        import logging

        from airflow.hooks.base import BaseHook
        from airflow.models import Variable
        from airflow.sdk import get_current_context

        from pipelines.nasa_donki_pipeline import load_donki

        log = logging.getLogger(__name__)
        context = get_current_context()
        params = context.get("params") or {}

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

        window: dict = {}
        if params.get("start_date") or params.get("end_date"):
            # Explicit backfill.
            window = {
                "start_date": params.get("start_date"),
                "end_date": params.get("end_date"),
            }
            log.info("Backfilling DONKI %s..%s in chunks.", window["start_date"], window["end_date"])
        else:
            # End the window at the run's own interval rather than at
            # wall-clock now. That is what makes the run idempotent: re-running
            # an interval requests the same dates and merges over its own
            # previous output. The pipeline's LOOKBACK_DAYS supplies the start.
            #
            # In Airflow 3.3 a MANUAL run has logical_date and data_interval set
            # to NULL, so this cannot be assumed present — same trap documented
            # in conjunction_screening.
            interval_end = getattr(context.get("dag_run"), "data_interval_end", None)
            if interval_end is None:
                log.warning(
                    "Manual run with no data interval and no date params. "
                    "Loading the trailing window from now; this run is NOT "
                    "reproducible."
                )
            else:
                window = {"end_date": interval_end.date().isoformat()}
                log.info("Loading DONKI window ending %s.", window["end_date"])

        return load_donki(
            api_key=Variable.get(NASA_API_KEY_VAR),
            credentials=credentials,
            **window,
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
