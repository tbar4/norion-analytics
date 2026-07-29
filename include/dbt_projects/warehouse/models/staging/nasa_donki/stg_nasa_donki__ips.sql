-- Interplanetary shocks — typed 1:1 view over raw.ips.
--
-- `location` is the observing vantage point (Earth, STEREO A, etc.), not a
-- position on the Sun — do not confuse it with source_location on CME/FLR.

select
    activity_id         as shock_id,
    event_time,
    location            as observed_from,
    catalog,
    submission_time,
    version_id,
    link,
    _dlt_load_id,
    _dlt_id
from {{ source('nasa_donki', 'ips') }}
