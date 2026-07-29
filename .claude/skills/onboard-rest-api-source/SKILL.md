---
name: onboard-rest-api-source
description: >
  Onboard a REST API as a new source in this data platform, end to end — dlt
  pipeline into Postgres `raw`, dbt staging and marts in `analytics`, an Airflow
  DAG that builds only that source's slice of the dbt graph, and a Cube model
  for the semantic layer. Use whenever a new API needs to land in the warehouse,
  or when asked to "add a source", "onboard <api>", "pull <api> into the
  warehouse", or "wire <api> up to Cube". This is the standing recipe — follow
  it rather than improvising a pipeline layout.
---

# Onboard a REST API source

A fixed five-layer recipe. The layers are declarative, so the process is too:
same file layout, same names, same order, every time.

**Do not improvise structure.** If something here does not fit the API, say so
and adapt deliberately — do not quietly invent a different layout.

## Naming — decide once, use everywhere

Pick `<source>`, snake_case, at the start. It is the same string in all of:

```
include/pipelines/<source>_pipeline.py      dlt pipeline module
  pipeline_name="<source>"                  dlt pipeline + state dir
  @dlt.source(name="<source>")              dlt source
models/staging/<source>/                    dbt folder
  source name in _<source>__sources.yml     dbt source
  +tags: ["<source>"] in dbt_project.yml    the DAG's selector
dags/<source>.py, dag_id="<source>"         Airflow DAG
```

Everything else is derived: `stg_<source>__<table>`, `<SOURCE>_API_KEY`.

## Before you start

Read `reference/platform.md`. It holds the topology, where credentials live,
and the gotchas that each cost a debugging session. Read
`reference/templates.md` for the skeleton of every file below.

`nasa_apod` is the complete worked example of all five layers. When a template
is ambiguous, read the real file.

---

## Step 0 — Gather inputs

Ask for anything not already given, **all at once**, then stop asking:

1. **API** — base URL and docs URL.
2. **Endpoints** — which ones, and what one row of each represents.
3. **Auth** — api key (query or header), bearer, basic, or OAuth2.
4. **Primary key** and **write disposition** per resource (`replace` for a
   re-fetched window, `merge` for incremental upserts, `append` for immutable
   events).
5. **Incremental cursor**, if any — the field and its format.
6. **Schedule** — cron. Check what else already calls the same host and give
   this source its own hour. Two DAGs sharing a slot is what makes an API 500
   under concurrent load, and the trigger is concurrency rather than the
   request quota. NASA slots in use: 06:00 apod, 07:00 donki, 08:00 neo_feed.
7. **Credential** — the Airflow Variable name to read the key from.

Prefer authoritative sources for API behaviour: the vendor's own docs, not
third-party wrappers. Use `WebSearch`/`WebFetch` when the shape is unclear —
guessing pagination costs more than looking it up.

## Step 1 — dlt pipeline

Write `include/pipelines/<source>_pipeline.py` from the template.

Non-negotiable, because each one is load-bearing here:

- `dataset_name="raw"` — the shared dlt dataset that dbt reads.
- `pipelines_dir=_state_dir()` — state on the bind mount, so it survives
  container restarts.
- Credentials and API key are **parameters with `None` defaults**, omitted from
  the source call when not passed. An explicit `None` overrides dlt's
  secrets.toml resolution instead of deferring to it.
- **No Airflow imports.** The module must stay runnable on the workstation.
- An `if __name__ == "__main__"` smoke test using `dev_mode=True`.

Set `data_selector` to match the response envelope (`"$"` for a bare root
array). Leave the paginator unset only if you have confirmed dlt detects it.

## Step 2 — Smoke test before touching Airflow

Run it directly, with `.add_limit()` or a narrow window if the API is large:

```bash
cd ~/docker/airflow && uv run python include/pipelines/<source>_pipeline.py
```

`dev_mode=True` lands in a throwaway timestamped dataset, so `raw` is
untouched. Confirm row counts and column types look right before continuing.
Debugging extraction is far cheaper here than through the scheduler.

Then remove the limit and let the real load into `raw` happen via the DAG.

## Step 3 — dbt

Four edits:

1. `models/staging/<source>/_<source>__sources.yml` — declare the raw tables.
   Source name is `<source>`, schema is always `raw`.
2. `models/staging/<source>/stg_<source>__<table>.sql` — one per raw table.
   Cast and rename only. Carry `_dlt_load_id` and `_dlt_id` through.
3. `models/staging/<source>/_<source>__models.yml` — the tests. **Tests on
   sources never run under Cosmos**; they must be on models.
4. `dbt_project.yml` — add the folder tag block under `models: warehouse:
   staging:`:

   ```yaml
         <source>:
           +tags: ["<source>"]
   ```

Add a mart under `models/marts/` only if there is something to model. Marts are
tables, read `ref()` never `source()`, and need no tag.

Verify the selector resolves what you expect before writing the DAG:

```bash
docker exec airflow-airflow-scheduler-1 bash -lc \
  'cd /opt/airflow/include/dbt_projects/warehouse && \
   /opt/dbt-venv/bin/dbt ls --select "tag:<source>+" --resource-type model \
     --profiles-dir . --project-dir .'
```

## Step 4 — Airflow DAG

Write `dags/<source>.py` from the template. The shape is always: one `load`
task, then a `DbtTaskGroup` selecting `tag:<source>+`.

- `RenderConfig` needs `invocation_mode=InvocationMode.SUBPROCESS` **and**
  `dbt_executable_path="/opt/dbt-venv/bin/dbt"`. The `DBT_RUNNER` default fails
  DAG parsing here — see `reference/platform.md`.
- `max_active_runs=1`.
- Import the pipeline **inside** the task function, so a broken pipeline module
  cannot break DAG parsing.
- Build the credentials dict from the `norion-analytics-pg` Connection.
  `conn.schema` is the **database** name — that is Airflow's naming, not ours.

Then confirm it parses and renders the right tasks:

```bash
docker exec airflow-airflow-scheduler-1 airflow dags reserialize
docker exec airflow-airflow-scheduler-1 airflow dags list-import-errors
docker exec airflow-airflow-scheduler-1 airflow tasks list <source>
```

You should see `load` plus a `.run` and `.test` per selected model — and
nothing belonging to another source. If other sources' models appear, the tag
block is wrong.

## Step 5 — Run it

```bash
docker exec airflow-airflow-scheduler-1 airflow dags unpause <source>
docker exec airflow-airflow-scheduler-1 airflow dags trigger <source>
```

Poll the metadata DB rather than the CLI — CLI output interleaves log lines
with data:

```bash
docker exec airflow-postgres-1 psql -U airflow -d airflow -t -A -F' | ' -c \
  "select task_id, state from task_instance
   where dag_id='<source>' order by start_date desc limit 10;"
```

On failure, read the task log under
`/opt/airflow/logs/dag_id=<source>/run_id=*/task_id=*/attempt=*.log`.

Confirm the data landed:

```bash
docker exec postgres_db psql -U tbarnes -d warehouse -c "\dt raw.*" \
  -c "\dt analytics.*" -c "\dv analytics.*"
```

## Step 6 — Cube (only once a mart exists)

Cubes are defined over dbt **marts**, never raw tables.

1. Write `semantic/cubes/<mart>.yml` **in this repo** from the template.
   `sql_table` must be schema-qualified: `analytics.<mart>`. The cube lives
   beside the dbt mart it reads so both change in one commit.
2. Dev Cube hot-reloads. Verify the model loads and, more importantly, that it
   can actually query:

   ```bash
   curl -s http://10.0.0.50:4001/cubejs-api/v1/meta | head -40
   curl -s -G 'http://10.0.0.50:4001/cubejs-api/v1/load' \
     --data-urlencode 'query={"measures":["<mart>.count"]}'
   ```

   A `meta` response only proves the YAML parsed. Run the `load` query — that
   is what proves the database, schema and column names are right.
3. Deploy to prod. `~/docker/cube/analytics` is a separate checkout of this
   repo — that separation is the promotion gate, so prod serves only what has
   been committed and pushed:

   ```bash
   cd ~/docker/airflow && git add semantic && git commit && git push
   cd ~/docker/cube/analytics && git pull --ff-only
   cd ~/docker/cube && docker compose restart cube_api cube_refresh_worker
   ```

   Prod requires JWT auth on the REST API, so verify from
   `docker logs cube-cube_api-1` (look for compilation errors) and
   `curl http://10.0.0.50:4000/readyz`. The SQL API on `:15432` needs no JWT,
   so a psql query against prod is the strongest check.

   Git identity is not configured globally on this host — commit with
   `git -c user.name=trevorb -c user.email=trevor.barnes91@gmail.com`.
4. Confirm the cube is usable from the two surfaces that actually work:

   - **Playground** at `10.0.0.50:4001` — the new cube should be selectable and
     return rows.
   - **psql** on the SQL API, where measures need `measure()`:

     ```bash
     psql -h 10.0.0.50 -p 15432 -U cube -d cube \
       -c "select measure(count) from <mart>;"
     ```

   **Do not try pgAdmin against Cube** — it cannot connect at all, for reasons
   in `reference/platform.md`. pgAdmin is for `warehouse` on `:5432` only.
   Metabase is no longer used; do not build anything against it.

   **Reconcile Cube against the warehouse.** Run this after every load that
   touches a cube with a pre-aggregation, not just when onboarding. The two
   numbers must be identical:

   ```bash
   # 1. the truth
   docker exec postgres_db psql -U tbarnes -d warehouse -t -A \
     -c "select count(*) from analytics.<mart>;"

   # 2. what Cube actually serves
   docker exec -e PGPASSWORD="$(grep -E '^CUBE_SQL_PASSWORD=' \
     ~/docker/cube/.env | cut -d= -f2-)" postgres_db \
     psql -h 10.0.0.50 -p 15432 -U cube -d cube -t -A \
     -c "select measure(count) from <mart>;"
   ```

   A mismatch means the rollup did not invalidate. Nothing errors when this
   happens — a stale pre-aggregation serves confidently wrong numbers, and this
   comparison is the only thing that catches it. On 2026-07-29 it caught Cube
   reporting 2,645 events when the warehouse held 16,573.
5. Add a **pre-aggregation** if the mart is large enough to warrant one. This
   is what the Cube Store cluster exists for; a cube without one makes Cube a
   hop that adds nothing.

   List only **additive** measures — `count`, `sum`, `min`, `max`. Additivity
   is what lets a single `granularity: day` rollup also answer monthly and
   yearly questions. Exact `count_distinct` and `avg` are non-additive and
   would pin the rollup to exactly its own dimensions; leave them out and let
   those queries fall through to Postgres.

   Do NOT add one to a small mart. `apod_daily` has 365 rows and deliberately
   has none — a rollup there is ceremony, not caching.

   **Key the `refresh_key` on the LOAD, not on an event timestamp.** This one
   fails silently and cost a real incident:

   ```yaml
   refresh_key:
     every: 1 hour
     sql: >
       SELECT MAX(_dlt_load_id::numeric)::text || ':' || COUNT(*)::text
       FROM analytics.<mart>
   ```

   A key like `MAX(submission_time)` looks better and is wrong. A backfill loads
   *old* rows whose business timestamp is also old, so the maximum never moves,
   so the rollup never invalidates — Cube serves pre-backfill numbers forever
   with no error. `_dlt_load_id` increases on every load regardless of the age
   of the data; `COUNT(*)` catches deletions; the `::numeric` cast avoids
   lexicographic comparison of a text column.

   Verify it is actually USED, never assume. `/v1/sql` returns the query plan,
   and the pre-aggregation table name must appear in the FROM clause:

   ```bash
   curl -s -G 'http://10.0.0.50:4001/cubejs-api/v1/sql' \
     --data-urlencode 'query={"measures":["<mart>.count"],"timeDimensions":[{"dimension":"<mart>.<time>","granularity":"month"}]}'
   ```

   On `/v1/load`, `external: true` with `extDbType: cubestore` means Cube Store
   served it. Note `usedPreAggregations` can come back null on a cache hit even
   when the rollup is working — the `/v1/sql` plan is the reliable check.

## Step 7 — Report

State plainly: rows landed in `raw`, models built in `analytics`, DAG run
result, and whether Cube serves the new model. If a step was skipped — no mart,
so no cube — say so rather than implying the whole chain is live.

Quote the **reconciliation** as two numbers, not as "Cube works": the warehouse
count and Cube's count, shown to match. "Cube returns data" is not evidence the
data is current — a stale pre-aggregation returns data too.

---

## Checklist

```
[ ] <source> named once, used in all 6 places
[ ] include/pipelines/<source>_pipeline.py — no Airflow imports, dataset_name="raw"
[ ] Smoke tested standalone with dev_mode=True
[ ] models/staging/<source>/ — sources.yml, stg_*.sql, models.yml (tests on MODELS)
[ ] dbt_project.yml — +tags block added
[ ] dbt ls --select "tag:<source>+" resolves the intended models
[ ] dags/<source>.py — SUBPROCESS render, max_active_runs=1, task-local import
[ ] DAG parses, renders only this source's models
[ ] DAG run green; rows present in raw and analytics
[ ] Cube model over a MART, load query returns data, deployed to prod
[ ] New cube returns rows in the Playground (:4001) and via psql on :15432
[ ] Cube's count RECONCILES with the warehouse count — a stale pre-aggregation
    serves wrong numbers without erroring
```

## When to deviate

- **An API with many endpoints** — one source, N resources. Do NOT create one
  pipeline per endpoint. If they share a base URL, an auth scheme and a
  paging/date contract, they belong in one `resources: [...]` list with the
  shared parts hoisted into `resource_defaults`. That gives one DAG, one dbt
  tag and one state directory. `nasa_donki` is the worked example: eleven DONKI
  services in a single source, driven by one `DONKI_RESOURCES` list so adding a
  service is a one-line change.
- **Nested arrays in the response** — dlt normalises each into its own child
  table (`<resource>__<array>`), so one resource can produce a dozen tables in
  `raw`. Stage the parent tables; leave the children in `raw` until something
  actually needs them, and say so in the sources file. `nasa_donki` lands ~42
  tables and stages 11.
- **Records that get revised** — key on the stable id and use `merge`. Do not
  put a version field in the primary key: that turns every revision into a new
  row, which is the opposite of what merge is for.
- **A resource with no natural id** — verify a composite key against live data
  rather than assuming one. DONKI's CMEAnalysis looked like it keyed on
  `associatedCMEID`, which was only 236/264 unique; the real grain needed
  `time21_5` as well.
- **No API key** (public endpoint) — drop the Variable and the `auth` block.
- **Not a REST API** — this recipe does not apply. SQL databases and
  files/object storage have their own dlt sources; steps 3–6 still hold, only
  step 1 changes.
- **Cross-source mart** — put it in `models/marts/` untagged. It rebuilds
  whenever any contributing source runs, which is the intent.
- **Source too large for one DAG run** — set up incremental loading in the dlt
  resource rather than shrinking the schedule.
