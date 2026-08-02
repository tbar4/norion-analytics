# Cube data model

The semantic layer. Lives in this repo, beside the dbt project it is built on,
so a change to a mart and the change to the cube that reads it land in **one
commit**.

That coupling is the whole reason this directory moved here on 2026-07-28. A
cube names a mart in `sql_table` and mirrors its columns; rename a column in
`include/dbt_projects/warehouse/models/marts/` and the cube breaks. When the
two lived in separate repos, no review could catch that.

Since 2026-08-02 CI checks the first half of that automatically. The `docs`
workflow resolves every cube's `sql_table` against the dbt manifest and
annotates the run when a cube names a table the project does not build; the
mismatch is also rendered on the published Cube reference page. It **warns
rather than fails**, on the grounds that a missing mart should degrade the docs
rather than block them — pass `--strict` to `scripts/build_docs_site.py` to
invert that. Note the limit: it compares *table* names, not columns. A renamed
column still gets through, and still needs the query in "Verifying a change"
below.

## Rules

- **Cubes are defined over dbt marts, never over `raw` dlt tables.** The raw
  layer is all-varchar and shaped by whatever the API returned. Casting and
  derived flags belong in dbt, where they are tested.
- `sql_table` must be schema-qualified — `analytics.<mart>`. Cube connects with
  no `search_path`, so a bare name resolves to `public` and fails.
- Cube hides primary keys by default. A cube whose only time dimension is the
  primary key needs `public: true` on it.

## Why this folder is called `semantic/` and not `cube/`

Cube's Python runtime imports a module named `cube` (`from cube import
TemplateContext`). A directory named `cube/` on a path Cube searches could
shadow it — the same class of bug as `include/dlt/` shadowing the dlt library,
which this platform has already been bitten by once. Do not rename it.

## How it is served

| Stack | Mounts | Purpose |
|---|---|---|
| `~/docker/cube-dev` | this directory, live | Edit here. Dev hot-reloads; Playground on `10.0.0.50:4001`. |
| `~/docker/cube` | `~/docker/cube/analytics/semantic`, a separate checkout | Prod. Deploy with a `git pull` in that checkout, then restart. |

Prod deliberately reads a **different checkout** rather than this working copy,
so an unfinished edit here cannot reach production. To deploy:

```bash
cd ~/docker/airflow && git add semantic && git commit && git push
cd ~/docker/cube/analytics && git pull --ff-only
cd ~/docker/cube && docker compose restart cube_api cube_refresh_worker
```

## Verifying a change

The Playground proves the model parses. Only a query proves the SQL is right:

```bash
psql -h 10.0.0.50 -p 15432 -U cube -d cube \
  -c "select measure(count) from <mart>;"
```

pgAdmin **cannot** connect to Cube's SQL API — see the platform reference in
`.claude/skills/onboard-rest-api-source/reference/platform.md`.

## Not yet adopted: cube_dbt

`cube_dbt` generates cube dimensions from dbt's `manifest.json`, removing the
hand-copied column lists here. It needs a custom Cube image (`pip install
cube_dbt`) and runtime access to `manifest.json`, which is a build artifact and
gitignored. Worth adopting once hand-maintaining dimensions starts causing
drift — around the third or fourth cube. Not before.
