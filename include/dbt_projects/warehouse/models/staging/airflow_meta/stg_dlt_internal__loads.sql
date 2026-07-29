-- dlt load packages — typed 1:1 view over raw._dlt_loads.
--
-- One row per load package. `schema_name` is dlt's source name, which is also
-- this platform's source name and the dbt tag — so it joins cleanly to the
-- Airflow side by convention.
--
-- `status` is dlt's own code: 0 means the package completed. It is an integer
-- rather than a label, so it is translated here instead of leaving every
-- consumer to remember what 0 means.

select
    load_id,
    schema_name             as source_name,
    inserted_at             as loaded_at,
    status                  as status_code,
    status = 0              as is_completed,
    schema_version_hash
from {{ source('dlt_internal', '_dlt_loads') }}
