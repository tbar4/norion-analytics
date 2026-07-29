# Templates

Copy these and substitute. `<source>` is the snake_case source name and is the
same string everywhere: pipeline module, dbt folder, dbt tag, DAG id, `dag_id`.

The worked example of all five layers is `nasa_apod`. When a template is
ambiguous, read the real file rather than guessing.

---

## 1. `include/pipelines/<source>_pipeline.py`

Imports nothing from Airflow, so it stays runnable and testable on the
workstation.

```python
"""<Source> -> Postgres schema `raw`.

<What the API is. Which endpoints. Where the docs are.>

<Any shape quirks: envelope vs bare array, pagination style, fields that are
conditionally absent and therefore nullable.>

Credentials are passed in by the caller. In Airflow that is the DAG, which
reads the `<SOURCE>_API_KEY` Variable and the `norion-analytics-pg`
Connection. When either argument is omitted, dlt falls back to
.dlt/secrets.toml, which exists only for running this module by hand.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Optional

import dlt
from dlt.sources.rest_api import RESTAPIConfig, rest_api_resources


def _state_dir() -> str:
    """Where dlt keeps load history and working files.

    Must survive container restarts, so it goes on the bind mount rather than
    the container's ephemeral home. Falls back to the repo copy when
    /opt/airflow is absent, which is what makes this runnable locally.
    """
    container_dir = Path("/opt/airflow/include/warehouse")
    base = (
        container_dir
        if container_dir.is_dir()
        else Path(__file__).resolve().parents[1] / "warehouse"
    )
    return str(base / ".dlt_pipelines")


@dlt.source(name="<source>")
def <source>_source(
    api_key: str = dlt.secrets.value,
    base_url: str = "https://api.example.com/v1/",
) -> Any:
    """<One line.>

    Args:
        api_key: Auto-loaded from secrets.toml under [sources.<source>].
        base_url: API root. Override only for testing against a mock.
    """
    config: RESTAPIConfig = {
        "client": {
            "base_url": base_url,
            "auth": {
                "type": "api_key",          # or bearer / basic / oauth2
                "name": "api_key",
                "api_key": api_key,
                "location": "query",        # or header
            },
            # Omit to let dlt auto-detect. Set explicitly once you know it.
            "paginator": {"type": "json_link", "next_url_path": "paging.next"},
        },
        "resource_defaults": {
            "primary_key": "id",
            "write_disposition": "merge",
        },
        "resources": [
            {
                "name": "<table>",
                "endpoint": {
                    "path": "<path>",
                    "data_selector": "data",   # "$" for a bare root array
                    "params": {},
                },
            },
        ],
    }

    yield from rest_api_resources(config)


def load_<source>(
    api_key: Optional[str] = None,
    credentials: Optional[dict] = None,
    dev_mode: bool = False,
) -> str:
    """Load <source> into the Postgres schema `raw`. Returns load info.

    Args:
        api_key: Omit to fall back to secrets.toml (local runs).
        credentials: Postgres connection as a dict of database/username/
            password/host/port. Omit to fall back to secrets.toml. The Airflow
            DAG builds this from the `norion-analytics-pg` Connection.
        dev_mode: Load into a fresh timestamped dataset instead of `raw`.
            Leave False for anything Airflow calls — dev datasets are
            invisible to dbt.
    """
    # Omitted args must stay *absent* rather than None, or an explicit None
    # would override dlt's secrets.toml resolution instead of deferring to it.
    source_kwargs: dict = {}
    if api_key is not None:
        source_kwargs["api_key"] = api_key

    destination = (
        dlt.destinations.postgres(credentials=credentials)
        if credentials
        else "postgres"
    )

    pipeline = dlt.pipeline(
        pipeline_name="<source>",
        destination=destination,
        dataset_name="raw",
        pipelines_dir=_state_dir(),
        dev_mode=dev_mode,
    )

    info = pipeline.run(<source>_source(**source_kwargs))
    return str(info)


if __name__ == "__main__":
    # Smoke test: isolated dataset, leaves `raw` untouched.
    print(load_<source>(dev_mode=True))  # noqa: T201
```

---

## 2. dbt

### `models/staging/<source>/_<source>__sources.yml`

```yaml
version: 2

sources:
  - name: <source>
    schema: raw
    description: >
      Landing tables written by include/pipelines/<source>_pipeline.py.
    tables:
      - name: <table>
        description: >
          <What one row is.>
        columns:
          - name: <col>
            description: <...>
```

The source NAME is per-source; the SCHEMA is always `raw`. That is what lets
every pipeline share one dlt dataset without their source declarations
colliding.

### `models/staging/<source>/stg_<source>__<table>.sql`

Renaming and casting only. No derived columns, no filtering, no joins — those
belong in the mart. Keeping this layer mechanical is what lets you diff it
against the source when a load looks wrong.

```sql
select
    cast(id as bigint)          as <table>_id,
    cast(created_at as timestamp) as created_at,
    some_column,
    _dlt_load_id,
    _dlt_id
from {{ source('<source>', '<table>') }}
```

Always carry `_dlt_load_id` and `_dlt_id` through every layer — they trace a
row back to the load that produced it.

### `models/staging/<source>/_<source>__models.yml`

Where the real tests go. Tests on a source never run under Cosmos.

```yaml
version: 2

models:
  - name: stg_<source>__<table>
    description: >
      <...>
    columns:
      - name: <table>_id
        data_tests:
          - not_null
          - unique
```

### `dbt_project.yml` — add the tag block

Under `models: warehouse: staging:`, add:

```yaml
      <source>:
        +tags: ["<source>"]
```

This is what the DAG's selector matches. Tagging the folder means new models
added to it inherit the tag with no further edits.

### `models/marts/<mart>.sql` (optional)

Only when there is something to model. A mart is a table, is business-facing,
reads `{{ ref('stg_...') }}` — never a source — and needs no tag: the `+` in
`tag:<source>+` reaches it. Document it in `models/marts/_marts__models.yml`.

---

## 3. `dags/<source>.py`

```python
"""<Source> -> Postgres schema `raw`, then this source's dbt models.

Credentials live in Airflow, not in .dlt/secrets.toml: the `<SOURCE>_API_KEY`
Variable and the `norion-analytics-pg` Connection.
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
API_KEY_VAR = "<SOURCE>_API_KEY"

# The dbt tag this source owns, set on the staging folder in dbt_project.yml.
SOURCE_TAG = "<source>"

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
# isolated in /opt/dbt-venv. See reference/platform.md.
render_config = RenderConfig(
    select=[f"tag:{SOURCE_TAG}+"],
    invocation_mode=InvocationMode.SUBPROCESS,
    dbt_executable_path="/opt/dbt-venv/bin/dbt",
)


@dag(
    dag_id="<source>",
    # Give this source its own hour if anything else already calls the same
    # host. Sharing a slot is what makes an API 500 under concurrent load — the
    # constraint is usually concurrency, not the request quota. Current NASA
    # slots: 06:00 apod, 07:00 donki, 08:00 neo_feed.
    schedule="<cron>",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    # One run at a time. Concurrent runs share the same dlt state directory and
    # hammer the same API.
    max_active_runs=1,
    tags=["dlt", "<source>", "raw"],
    doc_md=__doc__,
)
def <source>():
    # Retry at the ORCHESTRATOR layer, not just inside the request. dlt already
    # retries 429s and 5xx within a call, and that is not enough when an API is
    # down for minutes rather than seconds — nasa_apod exhausted dlt's attempts
    # after 2m52s and still failed. Coming back in ten minutes fixes it.
    @task(retries=3, retry_delay=timedelta(minutes=10))
    def load() -> str:
        # Imported inside the task so a broken pipeline module cannot stop the
        # whole DAG file from parsing.
        from airflow.hooks.base import BaseHook
        from airflow.models import Variable

        from pipelines.<source>_pipeline import load_<source>

        conn = BaseHook.get_connection(POSTGRES_CONN_ID)
        credentials = {
            "database": conn.schema,
            "username": conn.login,
            "password": conn.password,
            "host": conn.host,
            "port": conn.port or 5432,
        }

        return load_<source>(
            api_key=Variable.get(API_KEY_VAR),
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


<source>()
```

Note `conn.schema` is Airflow's field for the **database** name, not a Postgres
schema. That naming trap is Airflow's, not ours.

---

## 4. `semantic/cubes/<mart>.yml` (in this repo)

Always over a dbt **mart**, never a raw dlt table — the raw layer is
all-varchar and shaped by whatever the API returned.

```yaml
cubes:
  - name: <mart>
    sql_table: analytics.<mart>
    title: "<Human title>"
    description: >
      <What one row is.>

    dimensions:
      - name: <key>
        sql: <key>
        type: time            # or string / number / boolean
        primary_key: true
        # Cube hides primary keys by default; without this the field is
        # invisible in Metabase.
        public: true

      - name: <dim>
        sql: <dim>
        type: string

    measures:
      - name: count
        type: count

      - name: <filtered_count>
        type: count
        filters:
          - sql: "{CUBE}.<boolean_col>"
```

`sql_table` must be schema-qualified: Cube connects with no `search_path`, so a
bare name resolves to `public` and fails.
