-- Typed 1:1 view over raw.ll2_locations.
--
-- Renaming and casting only. The celestial body is carried as id and name;
-- its physical properties (mass, gravity, length of day) stay in `raw`.

select
    id                                  as location_id,
    name                                as location_name,
    active                              as is_active,
    description,
    cast(latitude as double precision)  as latitude,
    cast(longitude as double precision) as longitude,
    timezone_name,
    country__id                         as country_id,
    country__name                       as country_name,
    country__alpha_2_code               as country_alpha_2_code,
    country__alpha_3_code               as country_alpha_3_code,
    celestial_body__id                  as celestial_body_id,
    celestial_body__name                as celestial_body_name,
    total_launch_count,
    total_landing_count,
    _dlt_load_id,
    _dlt_id
from {{ source('launch_library', 'll2_locations') }}
