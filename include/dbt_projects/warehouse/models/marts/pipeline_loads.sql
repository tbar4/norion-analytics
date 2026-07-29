-- One row per dlt load package: what moved, how much, and which Airflow run
-- did it.
--
-- This is the "rows" half of platform observability. Airflow knows how long a
-- task took but has no idea whether it moved ten rows or ten thousand; dlt
-- knows the rows but nothing about schedules. Joining them is what makes
-- "yesterday's run was fast because it loaded nothing" visible.
--
-- THE JOIN IS BY CONVENTION, and worth understanding before trusting it.
-- Every pipeline in this platform is named for its source, and every source's
-- DAG is named the same — `nasa_donki` the dlt pipeline, `nasa_donki` the
-- dag_id. So dlt's `schema_name` matches Airflow's `dag_id` directly. A
-- pipeline whose name diverges from its dag_id would simply not match, and
-- would show a null run rather than a wrong one.
--
-- Time containment picks WHICH run: the load must have completed between the
-- run's start and end. `max_active_runs=1` on every DAG is what makes that
-- unambiguous — overlapping runs of one DAG would make it a coin toss.

with loads as (
    select * from {{ ref('stg_dlt_internal__loads') }}
),

rows_per_load as (
    select
        load_id,
        sum(row_count)      as row_count,
        count(*)            as table_count
    from {{ ref('stg_dlt_internal__load_rows') }}
    group by 1
),

runs as (
    select
        dag_run_id, dag_id, run_id, run_type, state, start_date, end_date
    from {{ ref('stg_airflow_meta__dag_run') }}
)

select
    l.load_id,
    l.source_name,
    l.loaded_at,
    cast(l.loaded_at as date)           as loaded_date,
    l.is_completed,
    l.status_code,

    -- Null when a merge rewrote nothing, or when the load's tables have since
    -- been dropped. Not the same as zero rows arriving.
    r.row_count,
    r.table_count,

    -- Null when no Airflow run brackets this load: a pipeline run by hand
    -- (a backfill, a smoke test) rather than by the scheduler. That is useful
    -- signal in itself, not a defect.
    d.dag_run_id,
    d.run_id,
    d.dag_id,
    d.run_type,
    d.state                             as run_state,
    d.dag_run_id is null                as was_run_outside_airflow,
    extract(epoch from (d.end_date - d.start_date)) as run_duration_seconds

from loads l
left join rows_per_load r on r.load_id = l.load_id
left join runs d
       on d.dag_id = l.source_name
      and l.loaded_at >= d.start_date
      -- A run still in flight has no end_date; treat it as open-ended so the
      -- load it is producing right now still matches.
      and l.loaded_at <= coalesce(d.end_date, now())
