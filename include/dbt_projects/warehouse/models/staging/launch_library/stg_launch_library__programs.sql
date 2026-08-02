-- Typed 1:1 view over raw.ll2_programs.
--
-- Renaming and casting only. A program's agencies are an ARRAY, so they live
-- in the child table `raw.ll2_programs__agencies` rather than here.

select
    id                                  as program_id,
    name                                as program_name,
    description,
    type__id                            as program_type_id,
    type__name                          as program_type_name,
    cast(start_date as timestamptz)     as start_date,
    cast(end_date as timestamptz)       as end_date,
    info_url,
    wiki_url,
    _dlt_load_id,
    _dlt_id
from {{ source('launch_library', 'll2_programs') }}
