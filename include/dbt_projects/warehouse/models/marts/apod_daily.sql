-- Business-facing Astronomy Picture of the Day — one row per calendar day.
--
-- This is the layer Cube and Metabase read. It is a table, so a dashboard
-- query reads precomputed results rather than re-running the SQL.
--
-- Derived columns live here rather than in staging: public-domain entries have
-- no copyright holder and videos have no HD still. Both are normal, so they
-- are surfaced as explicit flags rather than leaving every caller to
-- rediscover the NULL semantics.

select
    apod_date,
    title,
    media_type,
    url,
    hdurl,
    thumbnail_url,
    copyright,
    explanation,
    copyright is null       as is_public_domain,
    media_type = 'video'    as is_video,
    length(explanation)     as explanation_length,
    _dlt_load_id,
    _dlt_id
from {{ ref('stg_nasa_apod__apod') }}
