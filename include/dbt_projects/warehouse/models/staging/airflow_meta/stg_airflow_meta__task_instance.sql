-- Task attempts — typed 1:1 view over raw.airflow_task_instance.
--
-- Renaming and light derivation only; dlt reflected real types from the source
-- database, so there is nothing to cast.
--
-- IMPORTANT: this holds the LATEST attempt per task only. Airflow moves earlier
-- tries into task_instance_history, so a task that failed twice and then
-- succeeded appears here as a single success. `try_number` is what reveals that
-- — it is the count of attempts made, so > 1 means the task retried.

select
    id                          as task_instance_id,
    dag_id,
    run_id,
    task_id,
    task_display_name,
    map_index,
    state,
    try_number,
    max_tries,
    start_date,
    end_date,
    queued_dttm                 as queued_at,
    scheduled_dttm              as scheduled_at,
    -- Seconds. Null while a task is running or before it starts.
    duration                    as duration_seconds,
    operator,
    custom_operator_name,
    pool,
    pool_slots,
    queue,
    priority_weight,
    hostname,
    executor,
    -- Populated by Airflow when a retry is scheduled; the closest thing the
    -- metadata database has to an error message. Task logs live on disk, not
    -- in Postgres, so this is as much detail as SQL can offer.
    retry_reason,
    retry_delay_override,
    updated_at,
    _dlt_load_id,
    _dlt_id
from {{ source('airflow_meta', 'airflow_task_instance') }}
