-- Solar flares — typed 1:1 view over raw.flr.
--
-- Staging renames only; dlt already inferred correct types from DONKI's ISO
-- timestamps, so unlike the APOD source there is nothing to cast here.
--
-- class_type is the GOES classification (A/B/C/M/X plus a magnitude, e.g.
-- "X2.2"). Its first character is the order of magnitude, which is why the
-- mart splits it out rather than leaving callers to parse the string.

select
    flr_id              as flare_id,
    begin_time,
    peak_time,
    end_time,
    class_type,
    source_location,
    active_region_num,
    catalog,
    note,
    submission_time,
    version_id,
    link,
    _dlt_load_id,
    _dlt_id
from {{ source('nasa_donki', 'flr') }}
