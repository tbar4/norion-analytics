-- Blog posts — typed 1:1 view over raw.snapi_blogs. Identical shape to
-- articles; SNAPI separates them by editorial category, not by structure.

select
    id                  as blog_id,
    title,
    url                 as blog_url,
    image_url,
    news_site,
    summary,
    published_at,
    updated_at,
    featured,
    _dlt_load_id,
    _dlt_id
from {{ source('spaceflight_news', 'snapi_blogs') }}
