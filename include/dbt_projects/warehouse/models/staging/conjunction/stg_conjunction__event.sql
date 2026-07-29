-- Conjunction events — typed 1:1 view over raw.conjunction_event.
--
-- The grain is (screening_run_id, primary_norad_cat_id, secondary_norad_cat_id).
-- The pair is stored with the lower catalogue number as "primary", which is an
-- ordering convention only — it carries no meaning about which object is at
-- risk or which one would manoeuvre.
--
-- Every uncertainty-derived column keeps its `estimated_` prefix through this
-- layer deliberately. Dropping it here is how a synthetic number quietly
-- becomes an authoritative-looking one three models downstream.

select
    screening_run_id
        || '-' || primary_norad_cat_id::text
        || '-' || secondary_norad_cat_id::text     as conjunction_key,

    screening_run_id,

    cast(primary_norad_cat_id as bigint)           as primary_norad_cat_id,
    primary_object_name,
    primary_catalogue_source,

    cast(secondary_norad_cat_id as bigint)         as secondary_norad_cat_id,
    secondary_object_name,
    secondary_catalogue_source,

    cast(tca as timestamptz)                       as tca,
    cast(miss_distance_km as double precision)     as miss_distance_km,
    cast(relative_speed_km_s as double precision)  as relative_speed_km_s,
    cast(coarse_separation_km as double precision) as coarse_separation_km,

    cast(estimated_collision_probability as double precision)
                                                   as estimated_collision_probability,

    cast(estimated_sigma_radial_km_primary as double precision)
                                                   as estimated_sigma_radial_km_primary,
    cast(estimated_sigma_intrack_km_primary as double precision)
                                                   as estimated_sigma_intrack_km_primary,
    cast(estimated_sigma_crosstrack_km_primary as double precision)
                                                   as estimated_sigma_crosstrack_km_primary,
    cast(estimated_sigma_radial_km_secondary as double precision)
                                                   as estimated_sigma_radial_km_secondary,
    cast(estimated_sigma_intrack_km_secondary as double precision)
                                                   as estimated_sigma_intrack_km_secondary,
    cast(estimated_sigma_crosstrack_km_secondary as double precision)
                                                   as estimated_sigma_crosstrack_km_secondary,

    covariance_source,
    cast(hard_body_radius_km as double precision)  as hard_body_radius_km,

    _dlt_load_id,
    _dlt_id
from {{ source('conjunction', 'conjunction_event') }}
