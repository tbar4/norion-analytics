-- One row per Airflow task attempt, joined to its DAG run.
--
-- The task-level view: which step is slow, which one fails, which retries.
-- Cube's `task_runs` cube is built on this.
--
-- Grain is the LATEST attempt per task, because that is what Airflow keeps in
-- task_instance — earlier tries are moved to task_instance_history, which is
-- not yet materialised. `try_number` still reveals that a retry happened, it
-- just cannot tell you what the earlier attempt did.

with tasks as (
    select * from {{ ref('stg_airflow_meta__task_instance') }}
),

runs as (
    select dag_run_id, dag_id, run_id, run_type, state as run_state, start_date as run_started_at
    from {{ ref('stg_airflow_meta__dag_run') }}
)

select
    t.task_instance_id,
    t.dag_id,
    t.run_id,
    t.task_id,
    t.task_display_name,
    t.map_index,
    t.state,
    -- coalesce is load-bearing throughout: `state` is NULL for a task Airflow
    -- has created but not yet scheduled — 20 of them on the first run here —
    -- and `NULL = 'failed'` is NULL, not false. Left uncoalesced these columns
    -- are nullable, every boolean filter silently drops those rows, and a
    -- not_null test on them fails.
    coalesce(t.state = 'success', false)    as is_success,
    coalesce(t.state = 'failed', false)     as is_failed,
    -- Distinct from is_failed on purpose: an upstream_failed task did not fail,
    -- it never ran. Counting the two together inflates the failure rate and
    -- points at the wrong task.
    coalesce(t.state = 'upstream_failed', false) as is_upstream_failed,
    coalesce(t.state in ('success', 'failed'), false) as is_finished,
    coalesce(t.try_number > 1, false)        as was_retried,
    t.try_number,
    t.max_tries,
    t.start_date,
    t.end_date,
    t.duration_seconds,
    t.queued_at,
    -- Time spent waiting for a slot rather than doing work.
    extract(epoch from (t.start_date - t.queued_at)) as queue_seconds,
    cast(t.start_date as date)              as run_date,
    -- Cosmos names every dbt model task `<model>.run` / `.test`, so the
    -- operator column is what separates ingestion work from transformation.
    t.operator,
    t.custom_operator_name,
    t.pool,
    t.queue,
    t.hostname,
    t.executor,
    -- As close to an error message as the metadata database gets; task logs are
    -- files on disk, not rows.
    t.retry_reason,

    r.dag_run_id,
    r.run_type,
    r.run_state,
    r.run_started_at,

    t._dlt_load_id,
    t._dlt_id
from tasks t
left join runs r on r.dag_id = t.dag_id and r.run_id = t.run_id
