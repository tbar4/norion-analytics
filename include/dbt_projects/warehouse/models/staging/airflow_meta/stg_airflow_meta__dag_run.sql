-- DAG runs — typed 1:1 view over raw.airflow_dag_run.
--
-- Renaming only; dlt reflected real types from the source database.
--
-- Note `logical_date` is Airflow's scheduling timestamp (what used to be called
-- execution_date), NOT when the run actually happened. For "when did this run",
-- use start_date. Confusing the two is the classic Airflow reporting mistake.

select
    id                          as dag_run_id,
    dag_id,
    run_id,
    run_type,
    state,
    logical_date,
    queued_at,
    start_date,
    end_date,
    data_interval_start,
    data_interval_end,
    run_after,
    last_scheduling_decision,
    -- Increments each time a run is cleared and re-run.
    clear_number,
    triggered_by,
    triggering_user_name,
    created_at,
    updated_at,
    _dlt_load_id,
    _dlt_id
from {{ source('airflow_meta', 'airflow_dag_run') }}
