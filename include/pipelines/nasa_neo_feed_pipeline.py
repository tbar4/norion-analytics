"""NASA NeoWs feed -> Postgres schema `raw`.

Near Earth Object Web Service. One row is one asteroid's close approach on one
date. Docs: https://api.nasa.gov/ (NeoWs section).

    GET https://api.nasa.gov/neo/rest/v1/feed?start_date=&end_date=&api_key=

Three shape quirks, each of which changes how this is configured:

1. **The date range is hard-capped at 7 days.** An 8-day span returns HTTP 400
   ("The Feed date limit is only 7 Days"), not a truncated result. So the
   trailing window here is 7 days inclusive and a longer backfill has to loop
   over successive windows rather than widening this one.

2. **`near_earth_objects` is a dict keyed by date, whose values are lists.**
   Two levels, so the selector needs both: `near_earth_objects.*[*]`. `.*`
   alone stops at the per-date *lists* and hands dlt a list where it expects a
   record; `$` would yield the whole envelope. Verified against the live API —
   `.*[*]` returns exactly `element_count` records.

3. **`links.next` is a paginator trap.** It exists and looks usable, but always
   points at the *following* week, forever — there is no end of data to walk
   to. A `json_link` paginator would loop until the rate limit stops it. The
   window is driven explicitly instead, so the response is one page.

Nested fields dlt will split out: `estimated_diameter` (kilometers/meters/
miles/feet, each with min/max) flattens into columns; `close_approach_data` and
`links` become child tables.

Credentials are passed in by the caller. In Airflow that is the DAG, which
reads the `NASA_API_KEY` Variable and the `norion-analytics-pg` Connection.
When either argument is omitted, dlt falls back to .dlt/secrets.toml, which
exists only for running this module by hand.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Optional

import dlt
from dlt.common.pendulum import pendulum
from dlt.sources.rest_api import RESTAPIConfig, rest_api_resources

# The API rejects anything wider. 7 days inclusive == a 6-day difference.
FEED_MAX_SPAN_DAYS = 6


def _state_dir() -> str:
    """Where dlt keeps load history and working files.

    Must survive container restarts, so it goes on the bind mount rather than
    the container's ephemeral home. Falls back to the repo copy when
    /opt/airflow is absent, which is what makes this runnable locally.
    """
    container_dir = Path("/opt/airflow/include/warehouse")
    base = (
        container_dir
        if container_dir.is_dir()
        else Path(__file__).resolve().parents[1] / "warehouse"
    )
    return str(base / ".dlt_pipelines")


def _hoist_close_approach_date(record: dict) -> dict:
    """Copy the close-approach date up to the top level of the record.

    This is a deliberate exception to "raw is a faithful copy". The merge key
    has to be (asteroid, approach date), because the same asteroid reappears in
    later windows with a different approach — keying on `id` alone would make
    each run overwrite the previous approach and silently destroy history.

    The date is only available nested inside `close_approach_data`, which dlt
    splits into a child table, so it would not otherwise exist as a column on
    the parent to key on. Within a feed response that list is filtered to the
    requested window and holds exactly one element (verified against the live
    API), so element 0 is the approach this row represents.

    Nothing is removed — `close_approach_data` still lands in full.
    """
    approaches = record.get("close_approach_data") or []
    record["close_approach_date"] = (
        approaches[0].get("close_approach_date") if approaches else None
    )
    return record


@dlt.source(name="nasa_neo_feed")
def nasa_neo_feed_source(
    api_key: str = dlt.secrets.value,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    base_url: str = "https://api.nasa.gov/neo/rest/v1/",
) -> Any:
    """Near Earth Object close approaches over a trailing 7-day window.

    Args:
        api_key: Auto-loaded from secrets.toml under [sources.nasa_neo_feed].
            Get a free key at https://api.nasa.gov/.
        start_date: First day, "YYYY-MM-DD". Defaults to 6 days before
            end_date. Spans wider than 7 days are rejected by the API.
        end_date: Last day, "YYYY-MM-DD". Defaults to today (UTC).
        base_url: API root. Override only for testing against a mock.
    """
    if end_date is None:
        end_date = pendulum.now("UTC").to_date_string()
    if start_date is None:
        start_date = (
            pendulum.parse(end_date).subtract(days=FEED_MAX_SPAN_DAYS).to_date_string()
        )

    config: RESTAPIConfig = {
        "client": {
            "base_url": base_url,
            "auth": {
                "type": "api_key",
                "name": "api_key",
                "api_key": api_key,
                "location": "query",
            },
        },
        "resources": [
            {
                "name": "neo_feed",
                # (asteroid, approach date) — see _hoist_close_approach_date.
                "primary_key": ["id", "close_approach_date"],
                "write_disposition": "merge",
                "endpoint": {
                    "path": "feed",
                    # Dict keyed by date -> list of asteroids. Both levels are
                    # needed: `.*` alone stops at the lists.
                    "data_selector": "near_earth_objects.*[*]",
                    # One page by construction: see the docstring on links.next.
                    "paginator": {"type": "single_page"},
                    "params": {
                        "start_date": start_date,
                        "end_date": end_date,
                    },
                },
                "processing_steps": [{"map": _hoist_close_approach_date}],
            },
        ],
    }

    yield from rest_api_resources(config)


def load_nasa_neo_feed(
    api_key: Optional[str] = None,
    credentials: Optional[dict] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    dev_mode: bool = False,
) -> str:
    """Load NeoWs feed into the Postgres schema `raw`. Returns load info.

    Args:
        api_key: Omit to fall back to secrets.toml (local runs).
        credentials: Postgres connection as a dict of database/username/
            password/host/port. Omit to fall back to secrets.toml. The Airflow
            DAG builds this from the `norion-analytics-pg` Connection.
        start_date: Override the window start, "YYYY-MM-DD".
        end_date: Override the window end, "YYYY-MM-DD".
        dev_mode: Load into a fresh timestamped dataset instead of `raw`.
            Leave False for anything Airflow calls — dev datasets are
            invisible to dbt.
    """
    # Omitted args must stay *absent* rather than None, or an explicit None
    # would override dlt's secrets.toml resolution instead of deferring to it.
    source_kwargs: dict = {"start_date": start_date, "end_date": end_date}
    if api_key is not None:
        source_kwargs["api_key"] = api_key

    destination = (
        dlt.destinations.postgres(credentials=credentials)
        if credentials
        else "postgres"
    )

    pipeline = dlt.pipeline(
        pipeline_name="nasa_neo_feed",
        destination=destination,
        dataset_name="raw",
        pipelines_dir=_state_dir(),
        dev_mode=dev_mode,
    )

    info = pipeline.run(nasa_neo_feed_source(**source_kwargs))
    return str(info)


if __name__ == "__main__":
    # Smoke test: isolated dataset, leaves `raw` untouched.
    print(load_nasa_neo_feed(dev_mode=True))  # noqa: T201
