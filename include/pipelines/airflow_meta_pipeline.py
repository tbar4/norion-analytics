"""Airflow's own metadata database -> Postgres schema `raw`.

Makes the platform observable from the warehouse: DAG run outcomes, task
durations, retries and parse errors, queryable in SQL and chartable through
Cube alongside the data those pipelines produce.

Source is the Airflow metadata Postgres (`airflow-postgres-1`, database
`airflow`) — a DIFFERENT instance from the warehouse. See
reference/platform.md: there are three Postgres instances on this host and
confusing them is the easiest serious mistake here.

## What is deliberately NOT ingested

The metadata database also holds `connection` and `variable` — Fernet-encrypted
credentials — plus `session`, `revoked_token` and the `ab_user` tables (login
data). None of them are listed below, and none should ever be added. This
module uses an explicit column ALLOWLIST per table rather than an exclusion
list, so a new column appearing upstream cannot silently start flowing into the
warehouse.

Also skipped on purpose:

  xcom      values are pickled blobs of arbitrary task return data
  log       the audit log; noisy, and adds little the run tables do not
  *.jsonb   conf, context_carrier, next_kwargs — arbitrary nested user data
  *.bytea   executor_config — pickled

## Table names are prefixed

Resources are named `airflow_*` rather than `dag_run` / `dag` / `job`, because
every source in this platform shares one `raw` schema. Unprefixed names that
generic would eventually collide with another source's table.

## Credentials

Both sides come from Airflow Connections, per the house rule:

    airflow-db            source — the Airflow metadata Postgres, port 5433
    norion-analytics-pg   destination — the warehouse, port 5432

The DAG builds a SQLAlchemy URL from `airflow-db` and passes it in.

Two things to know about that Connection. First, its `schema` field must be
`airflow` — for Postgres connections Airflow's `schema` means the DATABASE, and
it was originally set to `public`, which is a Postgres schema and not a database
that exists. It failed with `FATAL: database "public" does not exist`. Second,
it duplicates a credential Airflow already holds in
`AIRFLOW__DATABASE__SQL_ALCHEMY_CONN`, so the two can drift if the metadata
password is ever rotated — rotate both together.

This module imports nothing from Airflow, so it stays runnable and testable
outside the scheduler. The metadata DB is published on 10.0.0.50:5433, so a
by-hand run works from the workstation with the same URL shape.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Optional

import dlt
from dlt.sources.sql_database import sql_table

# One entry per ingested table. Adding a table means adding a row here.
#
#   name        resource/destination table name, prefixed
#   table       source table in the airflow database
#   key         primary key; merge target
#   cursor      incremental column, or None to load the table in full each run
#   columns     explicit ALLOWLIST — see the module docstring
AIRFLOW_TABLES: list[dict[str, Any]] = [
    {
        "name": "airflow_dag_run",
        "table": "dag_run",
        "key": "id",
        "cursor": "updated_at",
        "columns": [
            "id", "dag_id", "run_id", "run_type", "state",
            "logical_date", "queued_at", "start_date", "end_date",
            "data_interval_start", "data_interval_end", "run_after",
            "last_scheduling_decision", "clear_number",
            "triggered_by", "triggering_user_name", "created_at", "updated_at",
        ],
    },
    {
        # The centre of gravity: `duration` is seconds, and try_number vs
        # max_tries is how a retry becomes visible.
        "name": "airflow_task_instance",
        "table": "task_instance",
        "key": "id",
        "cursor": "updated_at",
        "columns": [
            "id", "dag_id", "run_id", "task_id", "map_index", "state",
            "try_number", "max_tries", "start_date", "end_date", "duration",
            "queued_dttm", "scheduled_dttm", "operator", "custom_operator_name",
            "task_display_name", "pool", "pool_slots", "queue",
            "priority_weight", "hostname", "executor", "retry_reason",
            "retry_delay_override", "updated_at",
        ],
    },
    {
        # Prior tries are MOVED here when a task retries, so task_instance only
        # ever holds the latest attempt. Without this table a retried task looks
        # like it always succeeded first time.
        "name": "airflow_task_instance_history",
        "table": "task_instance_history",
        "key": ["task_instance_id", "try_number"],
        "cursor": "updated_at",
        "columns": [
            "task_instance_id", "dag_id", "run_id", "task_id", "map_index",
            "try_number", "max_tries", "state", "start_date", "end_date",
            "duration", "queued_dttm", "scheduled_dttm", "operator",
            "task_display_name", "pool", "queue", "hostname", "executor",
            "retry_reason", "updated_at",
        ],
    },
    {
        # Small and mutable, with no reliable change cursor — a full refresh is
        # cheaper than tracking it.
        "name": "airflow_dag",
        "table": "dag",
        "key": "dag_id",
        "cursor": None,
        "columns": [
            "dag_id", "dag_display_name", "description", "owners",
            "is_paused", "is_stale", "fileloc", "relative_fileloc",
            "bundle_name", "timetable_summary", "timetable_description",
            "max_active_runs", "max_active_tasks", "has_import_errors",
            "last_parsed_time", "last_parse_duration",
            "next_dagrun", "next_dagrun_create_after",
        ],
    },
    {
        # Usually empty. Rows here mean a DAG file failed to parse, which is
        # invisible in dag_run because a broken DAG never produces runs.
        "name": "airflow_import_error",
        "table": "import_error",
        "key": "id",
        "cursor": None,
        "columns": ["id", "timestamp", "filename", "bundle_name", "stacktrace"],
    },
    {
        # Scheduler / triggerer / dag-processor liveness.
        "name": "airflow_job",
        "table": "job",
        "key": "id",
        "cursor": "latest_heartbeat",
        "columns": [
            "id", "dag_id", "state", "job_type", "start_date", "end_date",
            "latest_heartbeat", "executor_class", "hostname",
        ],
    },
]


def _state_dir() -> str:
    """Where dlt keeps load history and working files.

    Must survive container restarts, so it goes on the bind mount rather than
    in the container's ephemeral home. Falls back to the repo copy when
    /opt/airflow is absent, which is what makes this runnable locally.
    """
    container_dir = Path("/opt/airflow/include/warehouse")
    base = container_dir if container_dir.is_dir() else Path(__file__).resolve().parents[1] / "warehouse"
    return str(base / ".dlt_pipelines")


@dlt.source(name="airflow_meta")
def airflow_meta_source(source_credentials: str = dlt.secrets.value) -> Any:
    """Airflow metadata tables as dlt resources.

    Args:
        source_credentials: SQLAlchemy connection string for the Airflow
            metadata database. The DAG builds this from the `airflow-db`
            Connection.
    """
    for spec in AIRFLOW_TABLES:
        incremental = (
            dlt.sources.incremental(spec["cursor"]) if spec["cursor"] else None
        )
        # `replace` for the tables with no cursor: without one, merge would
        # accumulate rows that no longer exist upstream (a deleted DAG, a
        # cleared import error) and quietly misreport current state.
        write_disposition = "merge" if spec["cursor"] else "replace"

        resource = sql_table(
            credentials=source_credentials,
            table=spec["table"],
            schema="public",
            incremental=incremental,
            included_columns=spec["columns"],
            write_disposition=write_disposition,
            primary_key=spec["key"],
            # Reflect types from the database rather than sniffing values, so
            # an all-NULL column in a small table still lands with a real type.
            reflection_level="full",
        )
        yield resource.with_name(spec["name"])


def load_airflow_meta(
    source_credentials: Optional[str] = None,
    credentials: Optional[dict] = None,
    dev_mode: bool = False,
) -> str:
    """Load Airflow metadata into the Postgres schema `raw`. Returns load info.

    Args:
        source_credentials: SQLAlchemy URL for the Airflow metadata database.
            Omit to fall back to secrets.toml (local runs only).
        credentials: Warehouse Postgres connection as a dict of database/
            username/password/host/port. Omit to fall back to secrets.toml.
            The Airflow DAG builds this from the `norion-analytics-pg`
            Connection.
        dev_mode: Load into a fresh timestamped dataset instead of `raw`.
    """
    # Omitted args must stay *absent* rather than None, or an explicit None
    # would override dlt's secrets.toml resolution instead of deferring to it.
    source_kwargs: dict = {}
    if source_credentials is not None:
        source_kwargs["source_credentials"] = source_credentials

    destination = dlt.destinations.postgres(credentials=credentials) if credentials else "postgres"

    pipeline = dlt.pipeline(
        pipeline_name="airflow_meta",
        destination=destination,
        dataset_name="raw",
        pipelines_dir=_state_dir(),
        dev_mode=dev_mode,
    )

    info = pipeline.run(airflow_meta_source(**source_kwargs))
    return str(info)


if __name__ == "__main__":
    # Smoke test: isolated dataset, leaves `raw` untouched.
    print(load_airflow_meta(dev_mode=True))  # noqa: T201
