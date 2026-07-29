-- Rows written per dlt load, per destination table.
--
-- This is the answer to "how many rows did that run move". Airflow has no
-- concept of rows — it tracks task outcomes and durations only — but dlt stamps
-- `_dlt_load_id` on every row it writes, so the count is recoverable by
-- grouping.
--
-- The table list is DISCOVERED at compile time from information_schema rather
-- than hardcoded, so onboarding a new source starts producing row metrics with
-- no edit here. That is deliberate: a hand-maintained union would silently stop
-- covering the newest source, which is exactly when you most want the numbers.
--
-- Caveat worth knowing before trusting a number: under `merge`, `_dlt_load_id`
-- records the load that LAST touched a row, not the load that created it. So a
-- count here is "rows this load wrote or rewrote", and re-running a merge over
-- an unchanged window moves rows onto the newer load id. For `replace`
-- resources it is simply the rows that load wrote.
--
-- Tables are referenced by raw SQL rather than source(), because they are
-- resolved dynamically and cannot be declared. Lineage for these edges is
-- therefore invisible to dbt — accepted in exchange for the list maintaining
-- itself.

{% set raw_tables = [] %}
{% if execute %}
    {% set discover %}
        select table_name
        from information_schema.columns
        where table_schema = 'raw'
          and column_name = '_dlt_load_id'
          and table_name !~ '^_dlt_'
        order by table_name
    {% endset %}
    {% set raw_tables = run_query(discover).columns[0].values() %}
{% endif %}

{% if raw_tables | length == 0 %}

-- No raw tables carry _dlt_load_id yet. Emit an empty, correctly-typed result
-- so the model still builds rather than producing invalid SQL.
select
    cast(null as varchar)   as table_name,
    cast(null as varchar)   as load_id,
    cast(null as bigint)    as row_count
where false

{% else %}

{% for t in raw_tables %}
select
    '{{ t }}'       as table_name,
    _dlt_load_id    as load_id,
    count(*)        as row_count
from raw.{{ t }}
group by 1, 2
{% if not loop.last %}union all{% endif %}
{% endfor %}

{% endif %}
