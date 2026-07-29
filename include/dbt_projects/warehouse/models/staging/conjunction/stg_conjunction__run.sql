-- Screening runs — typed 1:1 view over raw.screening_run.
--
-- One row per execution of the screening engine. Carries the parameters the
-- run used, so a conjunction can always be traced back to the resolution and
-- thresholds that produced it.

select
    screening_run_id,

    cast(started_at as timestamptz)     as started_at,
    cast(completed_at as timestamptz)   as completed_at,
    cast(window_start as timestamptz)   as window_start,
    cast(window_hours as double precision)      as window_hours,

    -- Screening parameters. Kept per-run rather than assumed constant: the
    -- step/radius pair is the main tuning lever and changing it changes what
    -- the run could possibly have found.
    cast(coarse_step_s as double precision)     as coarse_step_s,
    cast(coarse_radius_km as double precision)  as coarse_radius_km,
    cast(miss_threshold_km as double precision) as miss_threshold_km,
    cast(fine_step_s as double precision)       as fine_step_s,
    cast(hard_body_radius_km as double precision) as hard_body_radius_km,

    -- Coverage facts.
    cast(objects_screened as bigint)            as objects_screened,
    cast(objects_failed_init as bigint)         as objects_failed_init,
    cast(candidate_pairs as bigint)             as candidate_pairs,
    cast(conjunctions_found as bigint)          as conjunctions_found,
    cast(element_set_twins_suppressed as bigint) as element_set_twins_suppressed,
    cast(candidate_cap_hit as boolean)          as candidate_cap_hit,
    catalogue_sources,

    -- A run is only trustworthy as a complete screen when it examined the whole
    -- window AND had debris coverage. Derived once here so no downstream model
    -- has to remember both conditions.
    (
        not cast(candidate_cap_hit as boolean)
        and catalogue_sources like '%space_track%'
    )                                           as is_complete_screen,

    _dlt_load_id,
    _dlt_id
from {{ source('conjunction', 'screening_run') }}
