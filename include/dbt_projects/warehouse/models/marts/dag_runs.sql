-- One row per Airflow DAG run, with its task outcomes rolled up.
--
-- The run-level view of platform health: did it run, how long did it take, how
-- many tasks failed. Cube's `dag_runs` cube is built on this.
--
-- Duration is computed from start_date/end_date rather than taken from Airflow,
-- because dag_run has no duration column — only task_instance does.

with runs as (
    select * from {{ ref('stg_airflow_meta__dag_run') }}
),

task_rollup as (
    select
        dag_id,
        run_id,
        count(*)                                          as task_count,
        count(*) filter (where state = 'success')         as tasks_succeeded,
        count(*) filter (where state = 'failed')          as tasks_failed,
        count(*) filter (where state = 'upstream_failed') as tasks_upstream_failed,
        count(*) filter (where state = 'skipped')         as tasks_skipped,
        -- try_number counts attempts, so > 1 means the task was retried. This
        -- is the only retry signal available until task_instance_history has
        -- rows — see the sources file.
        count(*) filter (where try_number > 1)            as tasks_retried,
        -- duration_seconds, not duration: the staging model renames it.
        sum(duration_seconds)                             as task_seconds_total,
        max(duration_seconds)                             as slowest_task_seconds
    from {{ ref('stg_airflow_meta__task_instance') }}
    group by 1, 2
),

dags as (
    select dag_id, dag_display_name, owners, schedule, is_paused, is_stale
    from {{ ref('stg_airflow_meta__dag') }}
)

select
    r.dag_run_id,
    r.dag_id,
    r.run_id,
    r.run_type,
    r.state,
    -- Terminal states only. A run that is queued or still going is neither a
    -- success nor a failure, and counting it as either skews every rate.
    --
    -- coalesce is load-bearing: `state` is NULL for a run Airflow has created
    -- but not yet scheduled, and `NULL = 'success'` is NULL, not false. Without
    -- this these columns are nullable and any boolean filter silently drops
    -- those rows.
    coalesce(r.state = 'success', false)            as is_success,
    coalesce(r.state = 'failed', false)             as is_failed,
    coalesce(r.state in ('success', 'failed'), false) as is_finished,
    r.start_date,
    r.end_date,
    r.queued_at,
    -- Wall-clock seconds. Null while the run is still going.
    extract(epoch from (r.end_date - r.start_date))  as duration_seconds,
    -- How long the run sat before starting — the signal for a scheduler that is
    -- saturated rather than a DAG that is slow.
    extract(epoch from (r.start_date - r.queued_at)) as queue_seconds,
    cast(r.start_date as date)                       as run_date,
    -- Airflow's scheduling timestamp, NOT when the run happened. Kept for
    -- joining back to Airflow, but do not chart against it.
    r.logical_date,
    r.clear_number,
    r.triggering_user_name,

    coalesce(t.task_count, 0)                        as task_count,
    coalesce(t.tasks_succeeded, 0)                   as tasks_succeeded,
    coalesce(t.tasks_failed, 0)                      as tasks_failed,
    coalesce(t.tasks_upstream_failed, 0)             as tasks_upstream_failed,
    coalesce(t.tasks_skipped, 0)                     as tasks_skipped,
    coalesce(t.tasks_retried, 0)                     as tasks_retried,
    t.task_seconds_total,
    t.slowest_task_seconds,

    d.dag_display_name,
    d.owners,
    d.schedule,
    d.is_paused,
    d.is_stale,

    r._dlt_load_id,
    r._dlt_id
from runs r
left join task_rollup t on t.dag_id = r.dag_id and t.run_id = r.run_id
-- Left join: a DAG deleted from disk keeps its run history, and dropping that
-- history would make the platform look like it had done less than it has.
left join dags d on d.dag_id = r.dag_id
