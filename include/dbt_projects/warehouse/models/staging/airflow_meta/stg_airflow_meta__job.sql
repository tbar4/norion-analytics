-- Airflow component jobs — typed 1:1 view over raw.airflow_job.
--
-- Scheduler, triggerer and dag-processor heartbeats. Useful for answering "was
-- the platform even up?" when a schedule silently produced no runs — a DAG that
-- never fired and a scheduler that was down look identical from dag_run alone.

select
    id                          as job_id,
    dag_id,
    job_type,
    state,
    start_date,
    end_date,
    latest_heartbeat,
    executor_class,
    hostname,
    _dlt_load_id,
    _dlt_id
from {{ source('airflow_meta', 'airflow_job') }}
