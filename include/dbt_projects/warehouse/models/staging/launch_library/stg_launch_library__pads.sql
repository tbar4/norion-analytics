-- Typed 1:1 view over raw.ll2_pads.
--
-- Renaming and casting only. A pad embeds its location and that location's
-- celestial body; the location columns kept here are the ones that identify it,
-- and stg_launch_library__locations is the full view of that entity.

select
    id                                  as pad_id,
    name                                as pad_name,
    active                              as is_active,
    description,
    cast(latitude as double precision)  as latitude,
    cast(longitude as double precision) as longitude,
    country__id                         as country_id,
    country__name                       as country_name,
    country__alpha_2_code               as country_alpha_2_code,
    country__alpha_3_code               as country_alpha_3_code,
    location__id                        as location_id,
    location__name                      as location_name,
    total_launch_count,
    orbital_launch_attempt_count,
    fastest_turnaround,
    info_url,
    wiki_url,
    map_url,
    _dlt_load_id,
    _dlt_id
from {{ source('launch_library', 'll2_pads') }}
