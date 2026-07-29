"""SGP4 conjunction screening -> Postgres schema `raw`, then the conjunction dbt models.

Unlike every other DAG here, the "load" task does not fetch anything. It reads
the staged catalogue out of `analytics`, propagates every object with SGP4,
finds close approaches, and writes what it found back into `raw`. The rest of
the shape is the standard one-source DAG.

Scheduled 30 minutes after `celestrak` so it screens against fresh element sets
rather than racing the load that produces them. There is deliberately no
cross-DAG sensor: a screen against slightly stale elements is still useful, and
a sensor would turn a missed CelesTrak refresh into a missed screen.

Runtime is dominated by the coarse pass — roughly 5 minutes for a 24-hour
window over ~11.5k objects at the default 2 s resolution, and it scales with
both the object count and the window. execution_timeout is set well above that
so a slow run fails loudly instead of hanging a worker indefinitely.
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
SOURCE_TAG = "conjunction"

DBT_PROJECT_DIR = Path("/opt/airflow/include/dbt_projects/warehouse")

# How far ahead to screen. A longer window finds more, costs proportionally
# more, and gets less trustworthy as SGP4 error grows with propagation time.
SCREENING_WINDOW_HOURS = 24

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
    dag_id="conjunction_screening",
    schedule="30 */8 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    # One run at a time. Two concurrent screens would share a dlt state
    # directory and pointlessly duplicate several minutes of CPU.
    max_active_runs=1,
    tags=["dlt", "conjunction", "raw", "sgp4"],
    doc_md=__doc__,
)
def conjunction_screening():
    # No retries. Unlike an API fetch, this task fails for deterministic
    # reasons — a missing catalogue, bad elements, a code error — and none of
    # them are fixed by running the same five-minute computation again.
    @task(execution_timeout=timedelta(minutes=45))
    def screen() -> str:
        # Imported inside the task so a broken pipeline module cannot stop the
        # whole DAG file from parsing.
        from airflow.hooks.base import BaseHook

        from pipelines.conjunction_screening_pipeline import load_conjunction_screening

        conn = BaseHook.get_connection(POSTGRES_CONN_ID)
        credentials = {
            # conn.schema is Airflow's field for the DATABASE name, not a
            # Postgres schema. That naming trap is Airflow's, not ours.
            "database": conn.schema,
            "username": conn.login,
            "password": conn.password,
            "host": conn.host,
            "port": conn.port or 5432,
        }

        return load_conjunction_screening(
            credentials=credentials,
            window_hours=SCREENING_WINDOW_HOURS,
        )

    dbt_models = DbtTaskGroup(
        group_id="dbt_warehouse",
        project_config=ProjectConfig(dbt_project_path=DBT_PROJECT_DIR),
        profile_config=profile_config,
        execution_config=execution_config,
        render_config=render_config,
    )

    screen() >> dbt_models


conjunction_screening()
