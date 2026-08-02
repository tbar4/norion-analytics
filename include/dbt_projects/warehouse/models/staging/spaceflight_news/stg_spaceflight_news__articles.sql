-- News articles — typed 1:1 view over raw.snapi_articles.
--
-- Staging renames only. dlt already inferred correct types from SNAPI's ISO
-- timestamps, so there is nothing to cast here.
--
-- published_at is NOT cleaned: 59 rows carry a 1970-01-01 epoch default from
-- upstream. Nulling them here would hide a real data quality problem behind a
-- layer that is supposed to be mechanical, so the mart flags them instead.

select
    id                  as article_id,
    title,
    url                 as article_url,
    image_url,
    news_site,
    summary,
    published_at,
    updated_at,
    featured,
    _dlt_load_id,
    _dlt_id
from {{ source('spaceflight_news', 'snapi_articles') }}
