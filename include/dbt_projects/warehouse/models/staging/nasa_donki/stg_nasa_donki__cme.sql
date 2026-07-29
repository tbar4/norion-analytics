-- Coronal mass ejections — typed 1:1 view over raw.cme.
--
-- Staging renames only; dlt already inferred correct types from DONKI's ISO
-- timestamps, so unlike the APOD source there is nothing to cast here.
--
-- The nested arrays on each event (instruments, cmeAnalyses, linkedEvents,
-- sentNotifications) live in their own raw.cme__* tables and are deliberately
-- not staged — see _nasa_donki__sources.yml.

select
    activity_id         as cme_id,
    start_time,
    source_location,
    active_region_num,
    catalog,
    note,
    submission_time,
    version_id,
    link,
    _dlt_load_id,
    _dlt_id
from {{ source('nasa_donki', 'cme') }}
