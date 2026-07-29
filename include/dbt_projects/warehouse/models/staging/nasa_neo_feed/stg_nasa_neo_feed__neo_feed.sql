-- Typed 1:1 view over raw.neo_feed.
--
-- Staging does renaming and casting only — no derived columns, no filtering,
-- no joins. The join to the approach geometry belongs in the mart.
--
-- Unlike raw.apod, this table is not all-varchar: NeoWs returns real JSON
-- numbers and booleans, so dlt inferred double precision and boolean. Only the
-- ids and the date need casting.
--
-- dlt adds _dlt_id and _dlt_load_id to every table; they are how you trace a
-- row back to the load that produced it, so they are carried through every
-- layer.

select
    cast(id as bigint)                                      as asteroid_id,
    cast(neo_reference_id as bigint)                        as neo_reference_id,
    cast(close_approach_date as date)                       as close_approach_date,
    name,
    nasa_jpl_url,
    absolute_magnitude_h,
    is_potentially_hazardous_asteroid,
    is_sentry_object,
    sentry_data,
    estimated_diameter__kilometers__estimated_diameter_min  as est_diameter_km_min,
    estimated_diameter__kilometers__estimated_diameter_max  as est_diameter_km_max,
    estimated_diameter__meters__estimated_diameter_min      as est_diameter_m_min,
    estimated_diameter__meters__estimated_diameter_max      as est_diameter_m_max,
    _dlt_load_id,
    _dlt_id
from {{ source('nasa_neo_feed', 'neo_feed') }}
