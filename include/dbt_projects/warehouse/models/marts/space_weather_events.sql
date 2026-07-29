-- One timeline of every OBSERVED space weather event from NASA DONKI.
--
-- This is the layer Cube reads. It is a table, so a dashboard query reads
-- precomputed results rather than re-running an eight-way union.
--
-- Grain: one row per observed event, keyed on event_type + event_id.
--
-- In practice DONKI ids already embed their type — "2025-07-29T04:00:00-CME-001"
-- — so event_id alone is unique today (verified: 2,645/2,645 across a year).
-- The pair is still the declared grain because that uniqueness is NASA's
-- naming convention, not a documented guarantee, and CME and IPS both deliver
-- their id in a field called `activityID`.
--
-- Three staged models are deliberately EXCLUDED because they are not observed
-- events and unioning them here would quietly corrupt every event count:
--
--   stg_nasa_donki__cme_analysis          an analysis OF a CME, several per CME
--   stg_nasa_donki__wsa_enlil_simulation  a model FORECAST, not an observation
--   stg_nasa_donki__notification          a human bulletin about events
--
-- Columns that only some event types carry (source_location, flare_class,
-- observed_from) are null elsewhere by design — that is the cost of a single
-- timeline, and it is why each is documented rather than left to be guessed.

with cme as (
    select
        'CME'               as event_type,
        cme_id              as event_id,
        start_time          as event_time,
        cast(null as timestamptz) as event_end_time,
        source_location,
        active_region_num,
        cast(null as varchar)     as flare_class,
        cast(null as varchar)     as observed_from,
        link, submission_time, _dlt_load_id, _dlt_id
    from {{ ref('stg_nasa_donki__cme') }}
),

flr as (
    select
        'FLR'               as event_type,
        flare_id            as event_id,
        begin_time          as event_time,
        end_time            as event_end_time,
        source_location,
        active_region_num,
        -- GOES class is a letter plus a magnitude, e.g. "X2.2". The letter is
        -- the order of magnitude and is what anyone actually filters on, so it
        -- is split out here rather than left for every caller to parse.
        left(class_type, 1) as flare_class,
        cast(null as varchar)     as observed_from,
        link, submission_time, _dlt_load_id, _dlt_id
    from {{ ref('stg_nasa_donki__flr') }}
),

gst as (
    select
        'GST'               as event_type,
        storm_id            as event_id,
        start_time          as event_time,
        cast(null as timestamptz) as event_end_time,
        cast(null as varchar)     as source_location,
        cast(null as bigint)      as active_region_num,
        cast(null as varchar)     as flare_class,
        cast(null as varchar)     as observed_from,
        link, submission_time, _dlt_load_id, _dlt_id
    from {{ ref('stg_nasa_donki__gst') }}
),

ips as (
    select
        'IPS'               as event_type,
        shock_id            as event_id,
        event_time,
        cast(null as timestamptz) as event_end_time,
        cast(null as varchar)     as source_location,
        cast(null as bigint)      as active_region_num,
        cast(null as varchar)     as flare_class,
        observed_from,
        link, submission_time, _dlt_load_id, _dlt_id
    from {{ ref('stg_nasa_donki__ips') }}
),

{% for ev in ['sep', 'mpc', 'rbe', 'hss'] %}
{{ ev }} as (
    select
        '{{ ev | upper }}'  as event_type,
        event_id,
        event_time,
        cast(null as timestamptz) as event_end_time,
        cast(null as varchar)     as source_location,
        cast(null as bigint)      as active_region_num,
        cast(null as varchar)     as flare_class,
        cast(null as varchar)     as observed_from,
        link, submission_time, _dlt_load_id, _dlt_id
    from {{ ref('stg_nasa_donki__' ~ ev) }}
),
{% endfor %}

unioned as (
    select * from cme
    union all select * from flr
    union all select * from gst
    union all select * from ips
    union all select * from sep
    union all select * from mpc
    union all select * from rbe
    union all select * from hss
)

select
    -- Surrogate key over the declared grain. Materialised as one column for two
    -- reasons: it carries a plain `unique` test without pulling in dbt_utils,
    -- which this project does not install, and Cube requires a single
    -- primary_key column — a composite would have to be re-derived there.
    event_type || ':' || event_id as event_key,
    event_type,
    event_id,
    event_time,
    event_end_time,
    source_location,
    active_region_num,
    flare_class,
    observed_from,
    -- Convenience for time-series rollups; Cube can also derive these, but
    -- having them precomputed keeps the pre-aggregation definitions simple.
    cast(event_time as date) as event_date,
    link,
    submission_time,
    _dlt_load_id,
    _dlt_id
from unioned
