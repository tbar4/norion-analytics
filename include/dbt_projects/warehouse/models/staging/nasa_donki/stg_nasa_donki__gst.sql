-- Geomagnetic storms — typed 1:1 view over raw.gst.
--
-- The Kp index readings that quantify each storm are a nested array and land
-- in raw.gst__all_kp_index, which is not staged. Stage it if storm severity
-- becomes something you need to model.

select
    gst_id              as storm_id,
    start_time,
    submission_time,
    version_id,
    link,
    _dlt_load_id,
    _dlt_id
from {{ source('nasa_donki', 'gst') }}
