-- Article -> Launch Library 2 launch cross-reference.
--
-- One row per (article, launch) pair, so an article covering three launches
-- appears three times. This is the seam between the two Space Devs sources:
-- launch_id joins to stg_launch_library__launches.launch_id.
--
-- The join back to the article goes through dlt's _dlt_parent_id, which
-- references the PARENT's _dlt_id — not the article's business key. That is
-- why stg_spaceflight_news__articles carries _dlt_id through.

select
    _dlt_parent_id      as article_dlt_id,
    launch_id,
    provider,
    _dlt_list_idx       as launch_ordinal,
    _dlt_id
from {{ source('spaceflight_news', 'snapi_articles__launches') }}
