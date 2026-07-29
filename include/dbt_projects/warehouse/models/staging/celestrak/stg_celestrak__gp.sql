-- Public GP element sets — typed 1:1 view over raw.gp.
--
-- The grain is (norad_cat_id, epoch): one row per element set, not one row per
-- object. Successive runs accumulate history rather than overwriting, because
-- the screening engine's pseudo-covariance is derived from the scatter across
-- an object's recent element sets.
--
-- element_set_key exists because this project has no dbt_utils, so there is no
-- unique_combination_of_columns test. A surrogate key is the only way to assert
-- the compound grain with the built-in `unique` test, and that assertion is
-- what would catch the pipeline's merge key silently accumulating duplicates.
--
-- Angles are all degrees and mean_motion is revolutions per day — the OMM
-- units, renamed here so no downstream model has to guess. sgp4 wants exactly
-- these units, so nothing is converted.

select
    norad_cat_id::text || '-' || to_char(epoch at time zone 'UTC', 'YYYYMMDD"T"HH24MISSUS')
                                as element_set_key,

    norad_cat_id,
    object_name,
    object_id                   as international_designator,
    epoch,

    -- Mean Keplerian elements, OMM units, unconverted.
    mean_motion                 as mean_motion_rev_per_day,
    eccentricity,
    inclination                 as inclination_deg,
    ra_of_asc_node              as raan_deg,
    arg_of_pericenter           as arg_of_pericenter_deg,
    mean_anomaly                as mean_anomaly_deg,

    -- Drag and its derivatives. bstar is what makes SGP4 decay the orbit; a
    -- wrong sign or magnitude here is the usual cause of a wildly wrong
    -- propagation.
    bstar,
    mean_motion_dot,
    mean_motion_ddot,

    ephemeris_type,
    classification_type,
    element_set_no,
    rev_at_epoch,

    celestrak_group,

    _dlt_load_id,
    _dlt_id
from {{ source('celestrak', 'gp') }}
