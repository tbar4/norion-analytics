-- Business-facing launch catalogue — one row per launch, past and scheduled.
--
-- This is the layer Cube reads. It is a table, so a dashboard query reads
-- precomputed results rather than re-running the joins.
--
-- Derived columns live here rather than in staging. Three of them encode
-- LL2 semantics that every caller would otherwise have to rediscover:
--
--   * `net_is_exact` — LL2's `net` is "No Earlier Than", and its precision is
--     described by a separate field rather than by the value. A net of
--     2026-12-31T00:00:00 with precision "Year" means "some time in 2026", not
--     midnight on New Year's Eve. Treating every net as exact is the single
--     easiest mistake to make with this source, so the flag is explicit.
--   * `is_success` / `is_failure` — status is free text with several values;
--     these collapse it to the question actually being asked. Anything still
--     scheduled or in progress is neither.
--   * `has_launched` — separates history from the forward manifest.
--
-- The launch already carries its pad, provider and rocket denormalised from
-- LL2, so the only join needed is to agencies, for the provider's country and
-- type. It is a LEFT join on purpose: under a request budget the launch
-- catalogue and the agency dimension do not necessarily complete in the same
-- run, and a launch with an agency that has not loaded yet should still appear.

with launches as (
    select * from {{ ref('stg_launch_library__launches') }}
),

agencies as (
    select * from {{ ref('stg_launch_library__agencies') }}
)

select
    l.launch_id,
    l.launch_name,
    l.slug,
    l.net,
    l.window_start,
    l.window_end,
    l.last_updated,

    l.status_name,
    l.status_abbrev,
    l.net_precision_name,

    l.mission_name,
    l.mission_type,
    l.orbit_name,
    l.orbit_abbrev,

    l.rocket_configuration_name,
    l.rocket_configuration_full_name,

    l.launch_service_provider_id,
    l.launch_service_provider_name,
    l.launch_service_provider_abbrev,
    l.launch_service_provider_type,
    a.agency_type_name          as provider_agency_type,
    a.founding_year             as provider_founding_year,

    l.pad_id,
    l.pad_name,
    l.pad_latitude,
    l.pad_longitude,
    l.pad_country_name,
    l.pad_location_name,

    l.fail_reason,
    l.webcast_live,

    -- See the header: net without its precision is misleading.
    l.net_precision_name in ('Second', 'Minute', 'Hour')    as net_is_exact,
    l.status_name = 'Launch Successful'                     as is_success,
    l.status_name in ('Launch Failure', 'Partial Failure')  as is_failure,
    l.net < current_timestamp                               as has_launched,
    nullif(l.fail_reason, '') is not null                   as has_fail_reason,

    l._dlt_load_id,
    l._dlt_id
from launches l
left join agencies a
    on l.launch_service_provider_id = a.agency_id
