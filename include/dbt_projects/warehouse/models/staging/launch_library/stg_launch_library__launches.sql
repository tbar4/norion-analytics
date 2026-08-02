-- Typed 1:1 view over raw.ll2_launches.
--
-- Staging does renaming and casting only — no derived columns, no filtering,
-- no joins. That belongs in the mart. Keeping this layer mechanical is what
-- lets you diff it against the source when a load looks wrong.
--
-- The raw table has 150 columns because dlt flattens every nested object LL2
-- returns, and a launch embeds its pad, that pad's location, and that
-- location's celestial body. Most of that is image and licence metadata. This
-- selects the columns that describe the launch; the rest stay in `raw` for
-- anyone who wants them.
--
-- Timestamps are cast explicitly rather than relied upon. dlt infers these
-- from ISO-8601 strings and gets them right, but the cast is free when the
-- column is already timestamptz and correct if a future load ever arrives as
-- text.

select
    id                                      as launch_id,
    name                                    as launch_name,
    slug,
    cast(net as timestamptz)                as net,
    cast(window_start as timestamptz)       as window_start,
    cast(window_end as timestamptz)         as window_end,
    cast(last_updated as timestamptz)       as last_updated,

    status__id                              as status_id,
    status__name                            as status_name,
    status__abbrev                          as status_abbrev,
    net_precision__name                     as net_precision_name,

    mission__id                             as mission_id,
    mission__name                           as mission_name,
    mission__type                           as mission_type,
    mission__description                    as mission_description,
    mission__orbit__name                    as orbit_name,
    mission__orbit__abbrev                  as orbit_abbrev,

    rocket__id                              as rocket_id,
    rocket__configuration__id               as rocket_configuration_id,
    rocket__configuration__name             as rocket_configuration_name,
    rocket__configuration__full_name        as rocket_configuration_full_name,
    rocket__configuration__variant          as rocket_configuration_variant,

    launch_service_provider__id             as launch_service_provider_id,
    launch_service_provider__name           as launch_service_provider_name,
    launch_service_provider__abbrev         as launch_service_provider_abbrev,
    launch_service_provider__type__name     as launch_service_provider_type,

    pad__id                                 as pad_id,
    pad__name                               as pad_name,
    cast(pad__latitude as double precision) as pad_latitude,
    cast(pad__longitude as double precision) as pad_longitude,
    pad__country__name                      as pad_country_name,
    pad__location__id                       as pad_location_id,
    pad__location__name                     as pad_location_name,

    probability,
    weather_concerns,
    webcast_live,
    failreason                              as fail_reason,
    launch_designator,

    orbital_launch_attempt_count,
    location_launch_attempt_count,
    pad_launch_attempt_count,
    agency_launch_attempt_count,

    _dlt_load_id,
    _dlt_id
from {{ source('launch_library', 'll2_launches') }}
