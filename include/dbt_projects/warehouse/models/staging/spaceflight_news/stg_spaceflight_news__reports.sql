-- Status reports — typed 1:1 view over raw.snapi_reports.
--
-- No `featured` column: SNAPI returns it for articles and blogs but not for
-- reports. The mart supplies a null in its place rather than pretending the
-- field exists here.

select
    id                  as report_id,
    title,
    url                 as report_url,
    image_url,
    news_site,
    summary,
    published_at,
    updated_at,
    _dlt_load_id,
    _dlt_id
from {{ source('spaceflight_news', 'snapi_reports') }}
