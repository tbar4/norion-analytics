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

CURRENTLY IN TEMPORARY HOURLY CATCH-UP MODE (set 2026-08-02).
-------------------------------------------------------------
Running hourly with max_requests=15 so it spends the rate-limit budget as it
accrues. A daily 60-request run uses only a sixth of the ~360 requests/day the
API allows, which would stretch the first pass over three days; hourly finishes
in ~10 hours and each run takes seconds instead of holding a worker asleep.

REVERTING, once `raw.ll2_launches` is complete and the dbt models are green:

    schedule           "0 * * * *"  ->  "0 10 * * *"
    max_requests       15           ->  60
    max_sleep_seconds  900          ->  3700
    execution_timeout  45 minutes   ->  6 hours

10:00 is the daily slot, clear of the NASA sources (06:00, 07:00, 08:00) and of
spaceflight_news at 09:00. Nothing else contends for this host, and
max_active_runs=1 stops the DAG overlapping itself either way.

UNTIL THE CATCH-UP FINISHES, THE dbt TASKS FAIL. The staging models reference
tables (ll2_pads, ll2_spacecraft, ll2_launches, ...) that have not loaded yet,
and dbt cannot select around a missing source. This is expected and
self-healing: each run lands more tables and more models go green. The `load`
task succeeding is the signal that the pipeline itself is healthy.

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
    # TEMPORARY — HOURLY CATCH-UP. Revert to "0 10 * * *" with
    # max_requests=60 and max_sleep_seconds=3700 once the catalogue is
    # complete (see the "REVERTING" note in the docstring).
    #
    # Daily runs waste the rate limit. The API allows ~360 requests/day and a
    # daily 60-request run uses a sixth of that, stretching a ~160-request
    # first pass over three days. Hourly runs of 15 spend the budget as it
    # accrues and finish in ~10 hours, and each run takes seconds rather than
    # holding a worker asleep for four hours.
    schedule="0 * * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    # One run at a time. Two concurrent runs would share one dlt state
    # directory AND compete for the same 15-requests-per-hour budget, which is
    # the fastest way to get both of them throttled.
    max_active_runs=1,
    tags=["dlt", "launch_library", "raw"],
    doc_md=__doc__,
    # max_sleep_seconds must be sized against the SCHEDULE, not the budget: an
    # hourly run allowed to sleep an hour would overlap its own next run, and
    # max_active_runs=1 would then start skipping runs. At 900 a run that finds
    # no budget gives up after 15 minutes and the next hour retries — free,
    # because the page checkpoints persist.
    params={"max_requests": 15, "force_refresh": False, "max_sleep_seconds": 900},
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
    # execution_timeout sits under the hourly interval so a run can never
    # overlap the next one. Under the daily setting this was 6 hours, sized for
    # a 60-request run that spends most of its life asleep.
    @task(retries=1, retry_delay=timedelta(minutes=10), execution_timeout=timedelta(minutes=45))
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
        # Fallback matches the CURRENT (hourly) mode, not the pipeline default.
        # Runs queued before the switch carry no max_sleep_seconds in their
        # conf; without this they would inherit 3700s, sleep past the 45-minute
        # execution_timeout, and be killed mid-extract — which is the one way
        # to actually lose the page checkpoints. At 900 they stop cleanly
        # instead. Raise this to 3700 when reverting to the daily schedule.
        max_sleep_seconds = float(params.get("max_sleep_seconds", 900))

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
            max_sleep_seconds=max_sleep_seconds,
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
