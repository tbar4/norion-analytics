-- Typed 1:1 view over raw.apod.
--
-- Staging does renaming and casting only — no derived columns, no filtering,
-- no joins. That belongs in the mart. Keeping this layer mechanical is what
-- lets you diff it against the source when a load looks wrong.
--
-- The raw table arrives all-varchar because dlt infers types from JSON, where
-- every value is a string. Casting happens here rather than in the pipeline so
-- the raw layer stays a faithful copy of what the API returned.
--
-- dlt adds _dlt_id and _dlt_load_id to every table; they are how you trace a
-- row back to the load that produced it, so they are carried through every
-- layer.

select
    cast(date as date)  as apod_date,
    title,
    media_type,
    url,
    hdurl,
    thumbnail_url,
    copyright,
    explanation,
    _dlt_load_id,
    _dlt_id
from {{ source('nasa_apod', 'apod') }}
