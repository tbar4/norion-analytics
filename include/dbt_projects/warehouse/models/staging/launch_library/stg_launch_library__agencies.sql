-- Typed 1:1 view over raw.ll2_agencies.
--
-- Renaming only. Image, logo and licence columns are left in `raw` — they are
-- presentation metadata and nothing downstream joins to them.
--
-- COUNTRY IS NOT HERE. Unlike pads and locations, which carry a single country
-- flattened into `country__*` columns, an agency's `country` is an ARRAY —
-- agencies can be multinational — so dlt normalises it into the child table
-- `raw.ll2_agencies__country`. Join to that if you need it; do not expect a
-- country column on this model.

select
    id                      as agency_id,
    name                    as agency_name,
    abbrev                  as agency_abbrev,
    type__id                as agency_type_id,
    type__name              as agency_type_name,
    administrator,
    founding_year,
    description,
    featured,
    _dlt_load_id,
    _dlt_id
from {{ source('launch_library', 'll2_agencies') }}
