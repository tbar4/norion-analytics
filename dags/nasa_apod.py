"""NASA Astronomy Picture of the Day -> Postgres schema `raw`.

One day per run. The day comes from the run's own data interval, so re-running
an interval fetches the same date and merges over its own previous row — the
run is idempotent. Backfills go through the `start_date`/`end_date` params,
which the loader slices into 30-day requests.

This replaced a trailing 365-day window on 2026-08-02. That window was the
cause of this DAG's 500s: api.nasa.gov takes over 30 seconds to answer a
year-wide APOD query and the gateway times it out. The query was valid — just
too slow, deterministically. See the pipeline module docstring for timings.

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
    # Load one specific past day by hand:
    #   airflow dags trigger nasa_apod -c '{"date": "2026-07-20"}'
    #
    # Or backfill a range, which the loader slices into 30-day requests:
    #   airflow dags trigger nasa_apod \
    #     -c '{"start_date": "1995-06-16", "end_date": "2025-08-01"}'
    params={"date": None, "start_date": None, "end_date": None},
)
def nasa_apod():
    # api.nasa.gov returns intermittent 500s. dlt already retries inside the
    # request (its default covers 429 and all 5xx) and that is not always
    # enough: on 2026-07-29 the task retried for 2m52s and still failed.
    #
    # So the retry that matters is at the orchestrator layer — come back in ten
    # minutes. dlt handles per-request blips; Airflow handles "the upstream was
    # down for a while".
    #
    # Note this is now a backstop rather than the main defence. The 500s that
    # dominated this DAG's history were not blips: a 365-day window takes over
    # 30s upstream and times out into a 500 *every* time, so no retry schedule
    # could ever have cleared them. One day per run is the actual fix.
    @task(retries=3, retry_delay=timedelta(minutes=10))
    def load() -> str:
        # Imported inside the task so a broken pipeline module can't stop the
        # whole DAG file from parsing.
        import logging

        from airflow.hooks.base import BaseHook
        from airflow.models import Variable
        from airflow.sdk import get_current_context

        from pipelines.nasa_apod_pipeline import load_apod

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

        common = {
            "api_key": Variable.get(NASA_API_KEY_VAR),
            "credentials": credentials,
        }

        # An explicit range wins: this is the backfill path.
        if params.get("start_date") or params.get("end_date"):
            log.info(
                "Backfilling APOD %s..%s in chunks.",
                params.get("start_date"),
                params.get("end_date"),
            )
            return load_apod(
                start_date=params.get("start_date"),
                end_date=params.get("end_date"),
                **common,
            )

        # Otherwise one day. The override exists so a single missed day can be
        # replayed without triggering a whole range.
        target = params.get("date")
        if target:
            log.info("Loading APOD for explicit date=%s.", target)
        else:
            # The run's own day, not wall-clock now. This is what makes the DAG
            # idempotent: re-running an interval asks for the same date and
            # merges over its own previous row.
            #
            # data_interval_END, not start. The interval for the 06:00 run on
            # day D spans D-1 06:00 .. D 06:00, so `start` would be yesterday
            # and the table would permanently trail a day behind. APOD publishes
            # at 00:00 ET (04:00-05:00 UTC), so by 06:00 UTC day D's entry is up.
            #
            # In Airflow 3.3 a MANUAL run has logical_date and data_interval set
            # to NULL, so this cannot be assumed present — same trap documented
            # in conjunction_screening.
            interval_end = getattr(context.get("dag_run"), "data_interval_end", None)
            if interval_end is None:
                # Ad-hoc manual run with no interval and no override. Fall back
                # to today so the trigger still does something useful, but say
                # plainly that this run is not reproducible.
                import pendulum

                target = pendulum.now("UTC").to_date_string()
                log.warning(
                    "Manual run with no data interval and no date param. "
                    "Loading today (%s); this run is NOT reproducible.",
                    target,
                )
            else:
                target = interval_end.date().isoformat()
                log.info("Loading APOD for data_interval_end date=%s.", target)

        return load_apod(date=target, **common)

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
