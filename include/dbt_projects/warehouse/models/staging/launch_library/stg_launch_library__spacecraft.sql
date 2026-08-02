-- Typed 1:1 view over raw.ll2_spacecraft.
--
-- One row per spacecraft airframe — a specific vehicle with a serial number,
-- not a class. The class is `spacecraft_config__*`. Renaming only.

select
    id                                      as spacecraft_id,
    name                                    as spacecraft_name,
    serial_number,
    description,
    in_space                                as is_in_space,
    is_placeholder,
    time_in_space,
    time_docked,
    flights_count,
    mission_ends_count,
    fastest_turnaround,
    status__id                              as status_id,
    status__name                            as status_name,
    spacecraft_config__id                   as spacecraft_config_id,
    spacecraft_config__name                 as spacecraft_config_name,
    spacecraft_config__type__name           as spacecraft_config_type_name,
    spacecraft_config__in_use               as spacecraft_config_in_use,
    spacecraft_config__agency__id           as operator_agency_id,
    spacecraft_config__agency__name         as operator_agency_name,
    spacecraft_config__agency__abbrev       as operator_agency_abbrev,
    _dlt_load_id,
    _dlt_id
from {{ source('launch_library', 'll2_spacecraft') }}
