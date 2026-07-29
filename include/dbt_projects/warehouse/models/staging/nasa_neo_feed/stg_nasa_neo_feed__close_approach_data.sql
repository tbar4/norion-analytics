-- Typed 1:1 view over raw.neo_feed__close_approach_data, dlt's child table of
-- neo_feed holding the approach geometry.
--
-- Renaming and casting only. The join back to the parent belongs in the mart.
--
-- The casts here are load-bearing, not cosmetic. NeoWs returns every
-- miss_distance and relative_velocity value as a JSON *string* ("74798253.9"),
-- not a number, so dlt lands them as varchar. Left uncast they sort
-- lexically — "9" > "74798253" — which silently produces wrong answers to
-- "what was the closest approach" rather than an error.
--
-- `_dlt_parent_id` joins to stg_nasa_neo_feed__neo_feed._dlt_id and is carried
-- through unchanged. The unit variants NASA returns are kept as separate
-- columns rather than collapsed to one, because which unit is convenient
-- depends on the question — lunar distances read naturally for near misses,
-- kilometres for everything else.

select
    _dlt_parent_id,
    cast(close_approach_date as date)                                  as close_approach_date,
    close_approach_date_full,
    cast(epoch_date_close_approach as bigint)                          as epoch_date_close_approach,
    orbiting_body,
    cast(miss_distance__kilometers as double precision)                as miss_distance_km,
    cast(miss_distance__lunar as double precision)                     as miss_distance_lunar,
    cast(miss_distance__astronomical as double precision)              as miss_distance_au,
    cast(relative_velocity__kilometers_per_second as double precision) as velocity_km_s,
    cast(relative_velocity__kilometers_per_hour as double precision)   as velocity_km_h,
    _dlt_id
from {{ source('nasa_neo_feed', 'neo_feed__close_approach_data') }}
