"""Spaceflight News API -> Postgres schema `raw`, then this source's dbt models.

SNAPI is the editorial counterpart to the orbital catalogues here: CelesTrak and
Space-Track say what is in orbit, Launch Library 2 says who launched it, and
this says what was written about it. Articles carry LL2 launch ids, so the two
Space Devs sources join.

Credentials: NONE. SNAPI is public and unauthenticated. Only the
`norion-analytics-pg` Connection is needed, for the warehouse.

Scheduled at 09:00, which keeps it clear of the three api.nasa.gov sources
(06:00 apod, 07:00 donki, 08:00 neo_feed). Different host, so the separation is
tidiness rather than necessity — but it costs nothing to keep the pattern.

THE FIRST RUN IS A FULL BACKFILL and takes a few minutes: ~39,000 items over
roughly 400 requests. That happens automatically because the pipeline treats a
resource with no cursor in state as needing the whole archive, so there is no
flag to remember to set. Every run after that reads forward from the cursor and
finishes in seconds.

Trigger with {"backfill": true} to force the whole archive again. That is safe
at any time — the walk is deterministic and the merge key makes re-loading a
no-op — but it is a repair tool, not a routine.
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
SOURCE_TAG = "spaceflight_news"

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
# isolated in /opt/dbt-venv. See .claude/skills/.../reference/platform.md.
render_config = RenderConfig(
    select=[f"tag:{SOURCE_TAG}+"],
    invocation_mode=InvocationMode.SUBPROCESS,
    dbt_executable_path="/opt/dbt-venv/bin/dbt",
)


@dag(
    dag_id="spaceflight_news",
    schedule="0 9 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    # One run at a time. Concurrent runs share the same dlt state directory,
    # and two runs advancing one cursor is how a gap gets created.
    max_active_runs=1,
    tags=["dlt", "spaceflight_news", "raw"],
    doc_md=__doc__,
    # Force a full archive walk. Off by default: the first run backfills on its
    # own because the cursor is empty, and after that re-walking 39,000 items
    # daily would be several hundred pointless requests.
    params={"backfill": False},
)
def spaceflight_news():
    # Retry at the ORCHESTRATOR layer, not just inside the request. dlt already
    # retries 5xx within a call, and that is not enough when an API is down for
    # minutes rather than seconds — nasa_apod exhausted dlt's attempts after
    # 2m52s and still failed. Coming back in ten minutes fixes it.
    #
    # A retry re-walks from the stored cursor, which is wasteful but safe: the
    # cursor only advances after a COMPLETE walk, so a failed attempt leaves it
    # untouched and nothing is skipped.
    @task(retries=3, retry_delay=timedelta(minutes=10), execution_timeout=timedelta(hours=2))
    def load() -> str:
        # Imported inside the task so a broken pipeline module cannot stop the
        # whole DAG file from parsing.
        from airflow.hooks.base import BaseHook
        from airflow.sdk import get_current_context

        from pipelines.spaceflight_news_pipeline import load_spaceflight_news

        context = get_current_context()
        backfill = bool((context["params"] or {}).get("backfill", False))

        conn = BaseHook.get_connection(POSTGRES_CONN_ID)
        credentials = {
            # conn.schema is Airflow's field for the DATABASE name.
            "database": conn.schema,
            "username": conn.login,
            "password": conn.password,
            "host": conn.host,
            "port": conn.port or 5432,
        }

        return load_spaceflight_news(credentials=credentials, backfill=backfill)

    dbt_models = DbtTaskGroup(
        group_id="dbt_warehouse",
        project_config=ProjectConfig(dbt_project_path=DBT_PROJECT_DIR),
        profile_config=profile_config,
        execution_config=execution_config,
        render_config=render_config,
    )

    load() >> dbt_models


spaceflight_news()
