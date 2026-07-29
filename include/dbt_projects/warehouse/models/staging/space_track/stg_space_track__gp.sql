-- Full on-orbit catalogue — typed view over raw.space_track_gp.
--
-- Column names here are load-bearing beyond the usual: the conjunction
-- screening engine unions this model with stg_celestrak__gp by NAME, so the
-- shared columns must match that model exactly. If you rename one here, rename
-- it there too or the union breaks.
--
-- Space-Track returns all JSON values as strings, so unlike the CelesTrak
-- staging model this one genuinely casts rather than mostly renaming.
-- nullif(x, '') guards the empty strings Space-Track uses for absent numerics.

select
    norad_cat_id::text || '-' || to_char(cast(epoch as timestamptz) at time zone 'UTC', 'YYYYMMDD"T"HH24MISSUS')
                                as element_set_key,

    cast(norad_cat_id as bigint)                    as norad_cat_id,
    object_name,
    object_id                                       as international_designator,
    cast(epoch as timestamptz)                      as epoch,

    -- Mean Keplerian elements, OMM units, unconverted. SGP4 wants exactly these.
    cast(nullif(mean_motion, '') as double precision)        as mean_motion_rev_per_day,
    cast(nullif(eccentricity, '') as double precision)       as eccentricity,
    cast(nullif(inclination, '') as double precision)        as inclination_deg,
    cast(nullif(ra_of_asc_node, '') as double precision)     as raan_deg,
    cast(nullif(arg_of_pericenter, '') as double precision)  as arg_of_pericenter_deg,
    cast(nullif(mean_anomaly, '') as double precision)       as mean_anomaly_deg,

    cast(nullif(bstar, '') as double precision)              as bstar,
    cast(nullif(mean_motion_dot, '') as double precision)    as mean_motion_dot,
    cast(nullif(mean_motion_ddot, '') as double precision)   as mean_motion_ddot,

    cast(nullif(ephemeris_type, '') as integer)              as ephemeris_type,
    classification_type,
    cast(nullif(element_set_no, '') as integer)              as element_set_no,
    cast(nullif(rev_at_epoch, '') as bigint)                 as rev_at_epoch,

    -- Catalogue metadata CelesTrak's GP feed does not carry. object_type is the
    -- one that matters most here: it is what distinguishes debris from payloads.
    object_type,
    rcs_size,
    country_code,
    cast(nullif(launch_date, '') as date)                    as launch_date,
    cast(nullif(decay_date, '') as date)                     as decay_date,

    -- Orbit geometry, precomputed by Space-Track. Useful for an apogee/perigee
    -- pre-filter without deriving it from mean motion.
    cast(nullif(period, '') as double precision)             as period_minutes,
    cast(nullif(apoapsis, '') as double precision)           as apoapsis_km,
    cast(nullif(periapsis, '') as double precision)          as periapsis_km,
    cast(nullif(semimajor_axis, '') as double precision)     as semimajor_axis_km,

    _dlt_load_id,
    _dlt_id
from {{ source('space_track', 'space_track_gp') }}
