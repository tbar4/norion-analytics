# Platform contract

Facts about this stack that the recipe depends on. Verify a claim here before
working around it — most were established by hitting the failure first.

## Topology

| Thing | Where | Notes |
|---|---|---|
| Airflow 3.3.0 | `~/docker/airflow`, UI on `10.0.0.50:8080` | LocalExecutor, FAB auth |
| Warehouse Postgres | container `postgres_db`, `10.0.0.50:5432` | postgres:17, user `tbarnes`, database **`warehouse`** |
| Airflow metadata Postgres | container `airflow-postgres-1`, `:5433` | postgres:16, user `airflow`. **Separate instance.** Never load data here. |
| Cube prod | `~/docker/cube`, `10.0.0.50:4000` | Auth on REST API, no Playground. SQL API on `:15432` needs no JWT. Mounts `~/docker/cube/analytics/semantic`, a separate checkout of this repo. |
| Cube dev | `~/docker/cube-dev`, `10.0.0.50:4001` | Dev mode: Playground on, **no auth**, hot-reloads. Mounts this repo's `semantic/` live. |
| pgAdmin | `10.0.0.50:5050` | Postgres admin/debugging only. **Cannot query Cube** — see below. |

Metabase (`:3000`) is still running but is **no longer the UI** — dropped
2026-07-28, too many upsells and distractions. Do not build anything new
against it.

## Query surfaces — three, by purpose

| Want | Use | Where |
|---|---|---|
| Explore cubes, build a query visually | **Cube Playground** | `10.0.0.50:4001` (dev, no auth) |
| SQL against the semantic layer | **psql** | `10.0.0.50:15432`, user `cube` |
| Admin, debugging, inspecting `raw.*` | **pgAdmin** | `10.0.0.50:5432`, user `tbarnes`, db `warehouse` |

**pgAdmin cannot connect to Cube's SQL API.** Verified 2026-07-28 against Cube
1.7.12 — do not retry it and do not tell anyone it should work.

Cube emulates only a thin slice of `pg_catalog`: enough for BI tools, which
read `information_schema` and then `SELECT`, and not enough for admin tools,
which introspect deeply. Two concrete failures:

- pgAdmin never finishes its connection handshake —
  `Table or CTE with name 'pg_show_all_settings' not found`. The query comes
  from pgAdmin's psycopg driver at connect time, *below* the UI, so there is no
  setting that skips it.
- Even in psql, `\d` (list relations) works but `\d <table>` fails with
  `Unsupported SQL binary operator ... "~"`.

Cube's docs say "if an application connects to PostgreSQL, it can connect to
Cube as well". That is an overclaim; treat it as applying to BI tools only.

Measures are called through `measure()`, not plain aggregates:

```sql
select measure(count), measure(video_count) from apod_daily;
-- 365 | 28 — identical to the REST API on :4000
```

That wrapper is the reason cubes still earn their place now Metabase is gone —
it is what stops `video_count` being re-derived as ad-hoc SQL each time.

Passwords are `CUBE_SQL_PASSWORD` in `~/docker/cube/.env` and the Airflow
Connection respectively. Do not echo either into a terminal.

There are three Postgres instances on this host (the third is `thesis-app-pg-1`,
unrelated). Loading into the wrong one is the easiest serious mistake here.

## Warehouse layout

Single database `warehouse`, two schemas:

- **`raw`** — written by dlt. One dataset (`dataset_name="raw"`), all pipelines
  share it. Tables are all-varchar because dlt infers types from JSON.
- **`analytics`** — written by dbt, read by Cube. Both staging views and mart
  tables land here; the `stg_` prefix separates the layers, not the schema.

## Credentials — Airflow is the source of truth

Nothing secret belongs in this repo.

| Secret | Lives in | Read by |
|---|---|---|
| Warehouse Postgres | Airflow Connection `norion-analytics-pg` | DAG (builds a dict for dlt) and Cosmos (generates dbt's profile) |
| Per-source API keys | Airflow Variable, e.g. `NASA_API_KEY` | DAG, passed into the pipeline as an argument |

`.dlt/secrets.toml` is a **local-workstation-only** fallback so a pipeline
module can be run by hand. Airflow never reads it.

`include/dbt_projects/warehouse/profiles.yml` is for manual `dbt` runs only.
Cosmos ignores it and builds its own profile from the Connection, which is why
the warehouse password is never written to disk in this repo.

**Never** print a secret into the terminal (`airflow connections get` includes
the password). To verify a connection, query the database instead.

## Hard-won gotchas

These each cost a debugging session. Do not re-derive them.

**PYTHONPATH shadowing.** `/opt/airflow/include` is on PYTHONPATH and is
searched *before* site-packages. A directory named `include/dlt/` or
`include/dbt/` would shadow the installed libraries and break `import dlt`.
That is why the directories are `pipelines/` and `dbt_projects/`.

**Cosmos rendering must use SUBPROCESS.** `RenderConfig.invocation_mode`
defaults to `DBT_RUNNER`, which runs dbt in-process and needs dbt importable
from the Airflow environment. It is not — dbt is isolated in `/opt/dbt-venv`.

Cosmos fails to detect that: its guard is `find_spec("dbt")`, which returns
`None` rather than raising, so `is_dbt_installed_in_same_environment()` returns
`True` and `DBT_RUNNER` stands. DAG parsing then dies on a message about
something else entirely:

> RenderConfig.dbt_executable_path is set, but it is not the same as the system
> dbt executable path. Do not set render_config.dbt_executable_path when using
> InvocationMode.DBT_RUNNER.

**Deleting `dbt_executable_path` is the wrong response.** Pass
`invocation_mode=InvocationMode.SUBPROCESS` **and**
`dbt_executable_path="/opt/dbt-venv/bin/dbt"` to `RenderConfig`. It does not
inherit the path from `ExecutionConfig`, and `get_system_dbt()` falls back to
the bare string `"dbt"` because nothing named dbt is on PATH.

**Tests must be on models, not only sources.** Cosmos's default
`TestBehavior.AFTER_EACH` renders a test task after each *model*. A test
attached to a source in a `_*__sources.yml` parses but never runs in Airflow.
Put the real assertions in `_*__models.yml`.

**No `+schema` in dbt_project.yml.** dbt *appends* a custom schema to the
profile's, so `+schema: staging` yields `analytics_staging`, not `staging`.

**The Postgres connection uses `10.0.0.50`, not a docker alias.** Airflow sits
on its own compose network; `postgres_db` lives on `postgres_default`. The LAN
address is what crosses the gap.

**Cube points at `warehouse`.** Both `~/docker/cube` and `~/docker/cube-dev`
set `CUBEJS_DB_NAME=warehouse`. It was `postgres` until 2026-07-28 — a database
holding nothing but an empty `public` schema — so Cube silently saw no tables.
If a new cube reports missing relations, check this first.

**`postgres_db` reports `unhealthy` and is fine.** Its healthcheck uses
`pg_isready -u tbarnes`; the flag is `-U`. Cosmetic, unfixed.

**Airflow 3 CLI:** `dag_id` is positional — `airflow dags list-runs <dag_id>`,
not `-d <dag_id>`. CLI output interleaves log lines with data, so for anything
scripted query the metadata DB directly.

## Rebuilding the image

`requirements.txt` (Airflow env: dlt, Cosmos) and `requirements-dbt.txt`
(isolated dbt venv) are baked in at build time:

```bash
cd ~/docker/airflow && docker compose build && docker compose up -d
```

A new dlt destination extra or dbt adapter needs a rebuild. New *pipelines*,
*models* and *DAGs* do not — those are bind-mounted.
