-- Close approaches found by SGP4 screening, joined to the run that found them.
--
-- One row per (run, object pair). This is what Cube reads.
--
-- READ THIS BEFORE ACTING ON A ROW. The probability column here is ESTIMATED
-- from synthetic covariance derived from TLE scatter, because TLEs carry no
-- covariance of their own. It is useful for triage and for ranking what
-- deserves attention. It is NOT a substitute for a CDM, and a manoeuvre
-- decision needs the real thing. The column names and risk_band values are
-- deliberately worded so that is hard to forget.

with events as (
    select * from {{ ref('stg_conjunction__event') }}
),

runs as (
    select * from {{ ref('stg_conjunction__run') }}
),

latest_run as (
    select screening_run_id
    from runs
    order by started_at desc
    limit 1
)

select
    e.conjunction_key,
    e.screening_run_id,

    e.primary_norad_cat_id,
    e.primary_object_name,
    e.secondary_norad_cat_id,
    e.secondary_object_name,

    e.tca,
    e.miss_distance_km,
    e.relative_speed_km_s,

    e.estimated_collision_probability,
    e.covariance_source,

    -- Combined 1-sigma in-track uncertainty, the dominant error direction for
    -- a TLE. Surfaced on its own because it is the number that most directly
    -- explains why a small miss distance still yields a small Pc.
    sqrt(
        power(e.estimated_sigma_intrack_km_primary, 2)
        + power(e.estimated_sigma_intrack_km_secondary, 2)
    )                                           as estimated_combined_sigma_intrack_km,

    -- Triage banding. Thresholds follow common screening practice (1e-4 as the
    -- usual manoeuvre-consideration threshold, 1e-5 as the watch level), but
    -- every band is prefixed "estimated" because the input is not an
    -- authoritative Pc.
    case
        when e.estimated_collision_probability >= 1e-4 then 'estimated_high'
        when e.estimated_collision_probability >= 1e-5 then 'estimated_elevated'
        when e.estimated_collision_probability >= 1e-6 then 'estimated_low'
        else 'estimated_negligible'
    end                                         as risk_band,

    -- Intra-constellation pairs dominate the raw count once a large
    -- constellation is in the catalogue, and their operator manoeuvres them
    -- against each other automatically — CelesTrak's own SOCRATES stopped
    -- screening them for exactly this reason. Flagged rather than filtered, so
    -- the exclusion is the reader's choice and stays visible.
    (
        split_part(e.primary_object_name, '-', 1)
        = split_part(e.secondary_object_name, '-', 1)
        and split_part(e.primary_object_name, '-', 1) <> ''
    )                                           as is_intra_constellation,

    e.primary_catalogue_source,
    e.secondary_catalogue_source,

    -- Run context, carried onto every event so a consumer never has to join
    -- back to judge whether the row came from a trustworthy screen.
    r.started_at                                as run_started_at,
    r.window_hours                              as run_window_hours,
    r.miss_threshold_km                         as run_miss_threshold_km,
    r.objects_screened                          as run_objects_screened,
    r.catalogue_sources                         as run_catalogue_sources,
    r.candidate_cap_hit                         as run_candidate_cap_hit,
    r.is_complete_screen                        as run_is_complete_screen,

    (e.screening_run_id = (select screening_run_id from latest_run))
                                                as is_latest_run,

    e._dlt_load_id,
    e._dlt_id

from events e
join runs r on r.screening_run_id = e.screening_run_id
