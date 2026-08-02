# Norion Analytics

A self-hosted data platform for space domain awareness. Airflow orchestrates
dlt pipelines into Postgres (`raw`), dbt models them into `analytics`, and Cube
serves the result as a semantic layer.

**📖 [Documentation site](https://tbar4.github.io/norion-analytics/)** — dbt
model docs, lineage, and the Cube semantic layer reference.

## Sources

| Source | What it is |
|---|---|
| `nasa_apod` | Astronomy Picture of the Day |
| `nasa_donki` | Space weather — CME, flares, geomagnetic storms, SEP, IPS |
| `nasa_neo_feed` | Near-Earth object close approaches |
| `celestrak` | General perturbations element sets |
| `space_track` | Full catalogue including debris, plus element-set history |
| `conjunction_screening` | *Computed.* All-on-all SGP4 screen over the catalogue |
| `airflow_meta` | The platform observing itself — DAG runs, task runs, rows moved |

Collision probabilities from the conjunction screen are **estimated** from
covariance derived from TLE scatter, not from a CDM. They are a triage signal
and must not drive a manoeuvre decision, which is why every such field keeps
`estimated` in its name.

## Layout

| Path | What lives there |
|---|---|
| `dags/` | One Airflow DAG per source. Each builds only its own slice of the dbt graph. |
| `include/pipelines/` | dlt pipelines. No Airflow imports, so they run on a workstation too. |
| `include/dbt_projects/warehouse/` | The dbt project — `stg_` models 1:1 over raw, marts business-facing. |
| `semantic/cubes/` | Cube model, versioned alongside the marts it reads. |
| `scripts/` | Build tooling, including the documentation site generator. |

## Documentation

The site is rebuilt and published to GitHub Pages by `.github/workflows/docs.yml`
on every push to `main` that touches the dbt project, the cubes, or the
generator.

It builds **without a warehouse connection** — the runner has no route to the
LAN address Postgres listens on — so the docs carry the model graph,
descriptions, tests and source SQL, but not warehouse-derived column types or
table statistics. Everything published is derived from committed files, so the
site is exactly as fresh as the last push.

To build it locally:

```bash
dbt parse         --project-dir include/dbt_projects/warehouse --profiles-dir include/dbt_projects/warehouse
dbt docs generate --project-dir include/dbt_projects/warehouse --profiles-dir include/dbt_projects/warehouse \
                  --static --no-compile --empty-catalog
python scripts/build_docs_site.py \
  --target include/dbt_projects/warehouse/target --cubes semantic/cubes --out site
```

Then open `site/index.html`. Drop `--no-compile --empty-catalog` when the
warehouse *is* reachable and the docs gain compiled SQL and column types —
`dbt parse` becomes unnecessary in that case.

## Credentials

Airflow Variables and Connections are the only credential store. Nothing secret
belongs in this repo: `profiles.yml` is for manual runs and reads its password
from the environment, and Cosmos builds dbt's profile at runtime from the
`norion-analytics-pg` Airflow Connection.
