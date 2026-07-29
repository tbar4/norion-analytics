-- Operator-supplied supplemental element sets — typed 1:1 view over raw.sup_gp.
--
-- Same shape as stg_celestrak__gp with two differences, both load-bearing:
--
--   * The grain is (object_id, epoch), NOT (norad_cat_id, epoch). The
--     supplemental feed returns a placeholder catalogue number — Starlink rows
--     come back as 100001 for objects not yet catalogued — so norad_cat_id is
--     renamed to norad_cat_id_placeholder to make an accidental join to the
--     public catalogue hard to write by mistake.
--
--   * rms is present here and absent from gp. It arrives as a quoted string in
--     the JSON, so unlike every other numeric it genuinely needs a cast.

select
    international_designator || '-' || to_char(epoch at time zone 'UTC', 'YYYYMMDD"T"HH24MISSUS')
                                as element_set_key,
    international_designator,
    epoch,
    object_name,

    -- Not a catalogue number. See header.
    norad_cat_id_placeholder,

    mean_motion_rev_per_day,
    eccentricity,
    inclination_deg,
    raan_deg,
    arg_of_pericenter_deg,
    mean_anomaly_deg,

    bstar,
    mean_motion_dot,
    mean_motion_ddot,

    rms_residual,

    ephemeris_type,
    classification_type,
    element_set_no,
    rev_at_epoch,

    celestrak_file,

    _dlt_load_id,
    _dlt_id
from (
    select
        object_id               as international_designator,
        epoch,
        object_name,
        norad_cat_id            as norad_cat_id_placeholder,

        mean_motion             as mean_motion_rev_per_day,
        eccentricity,
        inclination             as inclination_deg,
        ra_of_asc_node          as raan_deg,
        arg_of_pericenter       as arg_of_pericenter_deg,
        mean_anomaly            as mean_anomaly_deg,

        bstar,
        mean_motion_dot,
        mean_motion_ddot,

        -- Quoted string in the source JSON, hence the only real cast here.
        cast(nullif(rms, '') as double precision) as rms_residual,

        ephemeris_type,
        classification_type,
        element_set_no,
        rev_at_epoch,

        celestrak_file,

        _dlt_load_id,
        _dlt_id
    from {{ source('celestrak', 'sup_gp') }}
) renamed
