-- Typed 1:1 view over raw.ll2_astronauts.
--
-- Renaming and casting only. `age` is carried as LL2 reports it rather than
-- being recomputed from date_of_birth — recomputing is derivation, which
-- belongs in a mart, and the two disagree for the deceased.

select
    id                                  as astronaut_id,
    name                                as astronaut_name,
    bio,
    cast(date_of_birth as date)         as date_of_birth,
    cast(date_of_death as date)         as date_of_death,
    age,
    in_space                            as is_in_space,
    status__id                          as status_id,
    status__name                        as status_name,
    type__id                            as astronaut_type_id,
    type__name                          as astronaut_type_name,
    agency__id                          as agency_id,
    agency__name                        as agency_name,
    agency__abbrev                      as agency_abbrev,
    cast(first_flight as timestamptz)   as first_flight,
    cast(last_flight as timestamptz)    as last_flight,
    flights_count,
    landings_count,
    spacewalks_count,
    time_in_space,
    eva_time,
    wiki                                as wiki_url,
    _dlt_load_id,
    _dlt_id
from {{ source('launch_library', 'll2_astronauts') }}
