-- High speed streams — typed 1:1 view over raw.hss.
--
-- Staging renames only; dlt already inferred correct types from DONKI's ISO
-- timestamps, so unlike the APOD source there is nothing to cast here.
--
-- version_id is carried but deliberately NOT part of the key: the pipeline
-- merges on hss_id so a revised event overwrites rather than duplicating.

select
    hss_id         as event_id,
    event_time,
    submission_time,
    version_id,
    link,
    _dlt_load_id,
    _dlt_id
from {{ source('nasa_donki', 'hss') }}
