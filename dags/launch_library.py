"""Launch Library 2 -> Postgres schema `raw`, then this source's dbt models.

LL2 is the dimensional backbone: agencies, pads, launch vehicles, spacecraft,
astronauts and the launch catalogue itself. It is what lets an object in orbit
be traced back to who launched it, on what, and from where.

Credentials: NONE today. An API key is optional and would only raise the rate
limit. If one is ever obtained, set the `LL2_API_KEY` Airflow Variable and this
DAG picks it up — the pipeline sizes itself from whatever `/api-throttle/`
reports, so nothing else changes.

    airflow variables set LL2_API_KEY '<key>'

FIFTEEN REQUESTS PER HOUR, ANONYMOUS. That single fact drives this DAG's shape.
A complete pass over the onboarded scope costs ~155 requests, i.e. about ten
hours of budget, so a run cannot simply fetch everything. Instead:

  * Each run spends at most `max_requests` (default 60, roughly a 3-hour task
    that bursts the first 15 immediately and then waits out window rollovers).
  * Resources are drained cheapest-first and checkpoint their page offset in
    dlt state, so a run that stops mid-table resumes exactly there next time.
  * THE FIRST FEW DAILY RUNS ARE A ROLLING CATCH-UP. The ~29 config lookup
    tables complete in run one, the mid-size dimensions over runs one and two,
    and `launches` (7,954 rows, ~80 requests) over the following two or three.
    After that the catalogue is complete and a run costs ~10 requests.

Scheduled daily at 10:00, clear of the NASA sources (06:00, 07:00, 08:00) and
of spaceflight_news at 09:00. A catch-up run can still be in flight for a few
hours after that; nothing else contends for this host, and max_active_runs=1
stops it overlapping itself.

NO HISTORICAL BACKFILL, deliberately. These endpoints are dimensional — they
expose current state, not an append-only archive — so there is nothing to walk
a date range over. Only `launches` has a `last_updated` cursor, and that is for
reading changes forward, not for reconstructing history.

Trigger with {"max_requests": 200} to push a catch-up along faster, or with
{"force_refresh": true} to re-read everything ignoring the staleness rotation.
Both are repair tools. force_refresh in particular costs a full ~155-request
pass and should not be scheduled.
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
API_KEY_VAR = "LL2_API_KEY"

# The dbt tag this source owns, set on the staging folder in dbt_project.yml.
SOURCE_TAG = "launch_library"

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
    dag_id="launch_library",
    schedule="0 10 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    # One run at a time. Two concurrent runs would share one dlt state
    # directory AND compete for the same 15-requests-per-hour budget, which is
    # the fastest way to get both of them throttled.
    max_active_runs=1,
    tags=["dlt", "launch_library", "raw"],
    doc_md=__doc__,
    params={"max_requests": 60, "force_refresh": False},
)
def launch_library():
    # execution_timeout is sized for the rate limiter, not for the work. A
    # 60-request run spends most of its life asleep waiting for the hourly
    # window to roll over — roughly 3 hours of wall clock for a few seconds of
    # actual transfer. 6 hours leaves room for a triggered catch-up with a
    # raised max_requests without cutting it off mid-table.
    #
    # A retry is cheap and safe: checkpoints mean it resumes rather than
    # restarts, and the merge keys make any re-read a no-op.
    @task(retries=2, retry_delay=timedelta(minutes=30), execution_timeout=timedelta(hours=6))
    def load() -> str:
        # Imported inside the task so a broken pipeline module cannot stop the
        # whole DAG file from parsing.
        from airflow.hooks.base import BaseHook
        from airflow.models import Variable
        from airflow.sdk import get_current_context

        from pipelines.launch_library_pipeline import load_launch_library

        context = get_current_context()
        params = context["params"] or {}
        max_requests = int(params.get("max_requests", 60))
        force_refresh = bool(params.get("force_refresh", False))

        conn = BaseHook.get_connection(POSTGRES_CONN_ID)
        credentials = {
            # conn.schema is Airflow's field for the DATABASE name.
            "database": conn.schema,
            "username": conn.login,
            "password": conn.password,
            "host": conn.host,
            "port": conn.port or 5432,
        }

        # The key is optional and absent today. default_var=None keeps this
        # from raising, and the pipeline treats None as "call anonymously".
        api_key = Variable.get(API_KEY_VAR, default_var=None)

        return load_launch_library(
            api_key=api_key,
            credentials=credentials,
            max_requests=max_requests,
            force_refresh=force_refresh,
        )

    dbt_models = DbtTaskGroup(
        group_id="dbt_warehouse",
        project_config=ProjectConfig(dbt_project_path=DBT_PROJECT_DIR),
        profile_config=profile_config,
        execution_config=execution_config,
        render_config=render_config,
    )

    load() >> dbt_models


launch_library()
