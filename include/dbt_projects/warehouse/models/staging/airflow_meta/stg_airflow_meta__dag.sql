-- DAGs — typed 1:1 view over raw.airflow_dag.
--
-- Fully refreshed each load rather than merged: it is six rows, and a merge
-- would keep DAGs that have since been deleted, quietly overstating what the
-- platform runs.

select
    dag_id,
    dag_display_name,
    description,
    owners,
    is_paused,
    -- True when Airflow can no longer find the DAG file. A stale DAG still has
    -- history but will never run again.
    is_stale,
    fileloc,
    relative_fileloc,
    bundle_name,
    timetable_summary           as schedule,
    timetable_description       as schedule_description,
    max_active_runs,
    max_active_tasks,
    has_import_errors,
    last_parsed_time,
    last_parse_duration         as last_parse_seconds,
    next_dagrun                 as next_logical_date,
    next_dagrun_create_after    as next_run_after,
    _dlt_load_id,
    _dlt_id
from {{ source('airflow_meta', 'airflow_dag') }}
