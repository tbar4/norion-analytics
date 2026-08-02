-- One timeline of every published item from the Spaceflight News API.
--
-- This is the layer Cube reads. It is a table, so a dashboard query reads
-- precomputed results rather than re-running a three-way union and a join.
--
-- Grain: one row per published item, keyed on item_type + item_id. SNAPI
-- numbers articles, blogs and reports in SEPARATE id sequences — article 1662
-- and report 1662 both exist and are unrelated — so item_id alone is NOT
-- unique here and the pair is the real grain, not merely the declared one.
--
-- `featured` is null for reports by design: SNAPI returns the field for
-- articles and blogs only. A null therefore means "not applicable", not
-- "not featured", which is why it is left null rather than coalesced to false.

with articles as (
    select
        'article'           as item_type,
        article_id          as item_id,
        title,
        article_url         as item_url,
        image_url,
        news_site,
        summary,
        published_at,
        updated_at,
        featured,
        _dlt_id,
        _dlt_load_id
    from {{ ref('stg_spaceflight_news__articles') }}
),

blogs as (
    select
        'blog'              as item_type,
        blog_id             as item_id,
        title,
        blog_url            as item_url,
        image_url,
        news_site,
        summary,
        published_at,
        updated_at,
        featured,
        _dlt_id,
        _dlt_load_id
    from {{ ref('stg_spaceflight_news__blogs') }}
),

reports as (
    select
        'report'            as item_type,
        report_id           as item_id,
        title,
        report_url          as item_url,
        image_url,
        news_site,
        summary,
        published_at,
        updated_at,
        cast(null as boolean) as featured,
        _dlt_id,
        _dlt_load_id
    from {{ ref('stg_spaceflight_news__reports') }}
),

unioned as (
    select * from articles
    union all select * from blogs
    union all select * from reports
),

-- How many LL2 launches each article cross-references. Aggregated BEFORE the
-- join so a multi-launch article stays one row in the mart rather than
-- fanning out — the grain above is the contract.
launch_links as (
    select
        article_dlt_id,
        count(*) as linked_launch_count
    from {{ ref('stg_spaceflight_news__article_launches') }}
    group by article_dlt_id
)

select
    -- Surrogate key over the declared grain. Materialised as one column for two
    -- reasons: it carries a plain `unique` test without pulling in dbt_utils,
    -- which this project does not install, and Cube requires a single
    -- primary_key column — a composite would have to be re-derived there.
    u.item_type || ':' || u.item_id     as item_key,
    u.item_type,
    u.item_id,
    u.title,
    u.item_url,
    u.image_url,
    u.news_site,
    u.summary,
    u.published_at,
    -- Convenience for time-series rollups; Cube can also derive this, but
    -- having it precomputed keeps the pre-aggregation definition simple.
    cast(u.published_at as date)        as published_date,
    u.updated_at,
    u.featured,

    -- Upstream data quality, surfaced rather than silently repaired. 59
    -- articles carry a 1970-01-01 epoch default instead of a real publication
    -- date. Left in place so counts stay complete, but flagged so a time
    -- series can exclude them instead of growing a spike at the epoch.
    u.published_at < timestamp '1990-01-01' as has_placeholder_published_at,

    -- Zero rather than null for blogs and reports: the cross-reference only
    -- exists for articles, and "no linked launches" is the truthful answer for
    -- an item type that cannot have any.
    coalesce(l.linked_launch_count, 0)  as linked_launch_count,

    u._dlt_load_id,
    u._dlt_id
from unioned u
left join launch_links l
    on l.article_dlt_id = u._dlt_id
   and u.item_type = 'article'
