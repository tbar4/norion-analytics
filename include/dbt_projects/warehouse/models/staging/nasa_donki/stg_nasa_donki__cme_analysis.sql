-- CME analyses — typed 1:1 view over raw.cme_analysis.
--
-- Several analyses exist per CME, which is why the pipeline merges on
-- associated_cmeid + time21_5 rather than on a single id: associated_cmeid
-- alone was only 236/264 unique when measured against the live API.
--
-- Column names here fix two dlt normalisation artefacts that are unpleasant to
-- type: `associated_cmeid` (from associatedCMEID) and
-- `associated_cm_estart_time` (from associatedCMEstartTime, which the
-- normaliser split at the wrong boundary).

select
    associated_cmeid                as cme_id,
    time21_5                        as arrival_time_21_5_rs,
    associated_cm_estart_time       as cme_start_time,
    speed,
    half_angle,
    latitude,
    longitude,
    speed_measured_at_height,
    type                            as analysis_type,
    is_most_accurate,
    measurement_technique,
    feature_code,
    image_type,
    data_level,
    catalog,
    note,
    associated_cme_link,
    submission_time,
    version_id,
    link,
    _dlt_load_id,
    _dlt_id
from {{ source('nasa_donki', 'cme_analysis') }}
