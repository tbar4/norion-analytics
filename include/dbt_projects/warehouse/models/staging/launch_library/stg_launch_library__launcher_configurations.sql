-- Typed 1:1 view over raw.ll2_launcher_configurations.
--
-- One row per rocket configuration — the specific variant flown, not the
-- family. Renaming only.
--
-- The family is an ARRAY, so dlt puts it in the child table
-- `raw.ll2_launcher_configurations__families` rather than a column here.

select
    id                          as launcher_configuration_id,
    name                        as configuration_name,
    full_name                   as configuration_full_name,
    variant,
    active                      as is_active,
    reusable                    as is_reusable,
    is_placeholder,
    manufacturer__id            as manufacturer_agency_id,
    manufacturer__name          as manufacturer_name,
    manufacturer__abbrev        as manufacturer_abbrev,
    manufacturer__type__name    as manufacturer_type_name,
    info_url,
    wiki_url,
    _dlt_load_id,
    _dlt_id
from {{ source('launch_library', 'll2_launcher_configurations') }}
