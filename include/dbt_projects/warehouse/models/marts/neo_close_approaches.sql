-- One row per asteroid close approach to Earth.
--
-- Joins the asteroid attributes to the approach geometry that dlt split into a
-- child table. The join is on _dlt_parent_id, which is dlt's own row identity —
-- not on a natural key — because the child carries no asteroid id of its own.
--
-- An inner join is correct here rather than a left join: a feed row without
-- approach geometry would mean the API returned an asteroid with an empty
-- close_approach_data, which has never been observed and would be a genuine
-- upstream anomaly rather than a row worth surfacing with NULL distances. The
-- 1:1 relationship is asserted by tests on the staging models.

with asteroids as (
    select * from {{ ref('stg_nasa_neo_feed__neo_feed') }}
),

approaches as (
    select * from {{ ref('stg_nasa_neo_feed__close_approach_data') }}
)

select
    -- Surrogate key, and the declared grain of this table. The real key is the
    -- (asteroid, date) pair, but the built-in `unique` test takes a single
    -- column and this project does not install dbt_utils. Same approach as
    -- `event_key` in space_weather_events. This is the test that would catch
    -- the pipeline's merge key regressing to `id` alone, which would silently
    -- collapse every repeat visit of an asteroid into one row.
    a.asteroid_id || ':' || a.close_approach_date       as approach_key,
    a.asteroid_id,
    a.close_approach_date,
    a.name,
    a.nasa_jpl_url,
    a.is_potentially_hazardous_asteroid,
    a.is_sentry_object,

    ap.miss_distance_km,
    ap.miss_distance_lunar,
    ap.velocity_km_s,
    ap.velocity_km_h,
    ap.orbiting_body,
    ap.close_approach_date_full,

    a.absolute_magnitude_h,
    a.est_diameter_m_min,
    a.est_diameter_m_max,
    -- Midpoint of NASA's estimated range. The range comes from an assumed
    -- albedo, so this is a convenience for ranking by size, not a measurement.
    (a.est_diameter_m_min + a.est_diameter_m_max) / 2.0 as est_diameter_m_mid,

    -- Lunar distance is the intuitive scale for "how close is close": 1.0 is
    -- the Moon's orbit. Anything inside that is genuinely near.
    ap.miss_distance_lunar < 1.0                        as is_closer_than_moon,

    a._dlt_load_id,
    a._dlt_id
from asteroids a
join approaches ap
    on a._dlt_id = ap._dlt_parent_id
