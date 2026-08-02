"""Spaceflight News API (SNAPI) -> Postgres schema `raw`.

SNAPI aggregates space journalism from ~42 outlets into one feed. It is run by
The Space Devs, the same group behind Launch Library 2, and articles carry
cross-references into LL2 launch and event ids — which is what makes the two
sources worth having together.

Base: https://api.spaceflightnewsapi.net/v4/
Docs: https://api.spaceflightnewsapi.net/v4/docs/

Three resources, identical in shape and handled by one generic walker:

  resource        path        one row is                    rows (2026-08-02)
  snapi_articles  articles/   a news article                          35,436
  snapi_blogs     blogs/      a blog post                              2,056
  snapi_reports   reports/    a status report                          1,415

Table names are PREFIXED `snapi_`. All pipelines here share one dlt dataset
(`raw`), so table names must be unique across sources, and `articles` / `blogs`
/ `reports` are far too generic to claim unqualified in a shared namespace.

NO RATE LIMIT. SNAPI does not throttle, which is why this source does the full
historical backfill that launch_library deliberately does not.

WHY THE BACKFILL WALKS DATE WINDOWS INSTEAD OF PAGINATING
---------------------------------------------------------
The obvious backfill is `?limit=500` plus follow the `next` link 71 times. That
is WRONG here, and quietly so.

SNAPI's `ordering` does not break ties deterministically. 59 articles share a
`published_at` of 1970-01-01T00:00:00Z (an upstream epoch-default), and the
same offset returns different rows across calls — verified live 2026-08-02:

    ?limit=3&ordering=published_at  ->  [3406, 3347, 2971]
    ?limit=5&ordering=published_at  ->  [2204, 3406, 3347, 2971, 3626]

Row 2204 sorts first in one call and is absent from the other. Under offset
pagination that is not a cosmetic reordering: a row that moves across a page
boundary between two requests is SKIPPED, permanently and silently, and the
load still reports success.

So this module never paginates by offset. It walks half-open
`published_at` windows and RECURSIVELY HALVES any window whose `count` exceeds
the page limit, until every window fits in a single request. One request, one
complete window, no offsets, no ordering dependency. The densest month found
(2025-01, 534 articles) does exceed the 500-row page cap, so the splitting is
load-bearing rather than theoretical.

Windows are keyed on `published_at`, which never changes for a given article.
That is what makes the walk reproducible: the same run tomorrow visits exactly
the same windows and fetches exactly the same rows, and `merge` on `id` makes
re-loading them a no-op. That is the whole idempotency story.

INCREMENTAL RUNS use the same windowed walker, over `updated_at` instead, and
bounded above by the run's start time. Bounding above matters: an article
revised mid-walk moves out of the window rather than shuffling positions inside
it, and it is picked up on the next run because the cursor is deliberately
parked a lag behind. `updated_at` rather than `published_at` is what catches
REVISIONS of old articles, not just new ones.

THE CURSOR IS SET FROM THE CLOCK, NOT FROM THE DATA. After a complete walk,
every row that existed at `run_start` has been ingested, so the cursor becomes
`run_start - CURSOR_LAG`. Using `max(updated_at)` instead would be subtly
broken during a backfill, where old rows carry old timestamps and the maximum
never moves. Same trap as a Cube refresh_key on an event timestamp.

A NOTE ON SILENTLY IGNORED FILTERS. SNAPI answers an unknown query parameter
with 200 and the UNFILTERED result set — `?bogus_param=1` returns all 35,436
rows. A typo in a filter name therefore does not error, it silently widens the
query. `_check_filter_contract` probes for that at the start of a backfill
rather than letting it corrupt a load.

Credentials: none. SNAPI is public and needs no key. The Postgres credentials
are passed in by the caller; in Airflow that is the DAG, reading the
`norion-analytics-pg` Connection.

This module deliberately imports nothing from Airflow, so it stays runnable and
testable outside the scheduler.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterator, Optional

import dlt
from dlt.sources.helpers import requests

logger = logging.getLogger(__name__)

BASE_URL = "https://api.spaceflightnewsapi.net/v4/"

# The Space Devs ask callers to be identifiable rather than anonymous.
USER_AGENT = "norion-warehouse/1.0 (+trevor.barnes91@gmail.com)"

# SNAPI's hard page cap. Asking for 1000 returns 500, so this is the ceiling
# rather than a preference — verified live 2026-08-02.
PAGE_LIMIT = 500

REQUEST_TIMEOUT_SECONDS = 120

# resource name -> path. Table name == resource name.
SNAPI_RESOURCES: list[tuple[str, str]] = [
    ("snapi_articles", "articles"),
    ("snapi_blogs", "blogs"),
    ("snapi_reports", "reports"),
]

# Start of the backfill window. Deliberately BEFORE the 1970-01-01 epoch
# defaults that 59 articles carry — a window starting at, say, 2010 would drop
# them without ever reporting a gap. Verified: `published_at_gte=1900-01-01`
# returns 35,436, exactly the unfiltered total, so nothing sorts earlier than
# this and no row has a null published_at.
BACKFILL_START = datetime(1900, 1, 1, tzinfo=timezone.utc)

# How far behind the run start the incremental cursor is parked. Covers clock
# skew between this host and SNAPI, and the case of an article revised while
# the walk was in flight. Costs one re-read of a small window per run, which
# `merge` absorbs.
CURSOR_LAG = timedelta(hours=6)

# Guard against a pathological window that cannot be split any further. A
# window this narrow holding more than PAGE_LIMIT rows would mean 500+ articles
# sharing one second, which does not happen; if it ever does, the walker falls
# back to offset paging and says so rather than silently truncating.
MIN_WINDOW = timedelta(seconds=1)


def _state_dir() -> str:
    """Where dlt keeps load history and working files.

    Must survive container restarts, so it goes on the bind mount rather than
    in the container's ephemeral home. Falls back to the repo copy when
    /opt/airflow is absent, which is what makes this runnable locally.
    """
    container_dir = Path("/opt/airflow/include/warehouse")
    base = container_dir if container_dir.is_dir() else Path(__file__).resolve().parents[1] / "warehouse"
    return str(base / ".dlt_pipelines")


def _iso(moment: datetime) -> str:
    """SNAPI wants `2026-08-02T21:23:13Z`, not `+00:00`."""
    return moment.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _get(path: str, params: dict) -> dict:
    """One SNAPI request. Returns the decoded envelope.

    Uses dlt's requests helper rather than bare requests so 5xx and connection
    errors are retried inside the call; the DAG retries on top of that.
    """
    response = requests.get(
        f"{BASE_URL}{path}/",
        params=params,
        headers={"User-Agent": USER_AGENT},
        timeout=REQUEST_TIMEOUT_SECONDS,
    )
    response.raise_for_status()
    payload = response.json()
    if not isinstance(payload, dict) or "results" not in payload:
        raise RuntimeError(
            f"Unexpected SNAPI response shape for {path}: "
            f"{type(payload).__name__} with keys {list(payload)[:5] if isinstance(payload, dict) else 'n/a'}"
        )
    return payload


def _check_filter_contract(path: str) -> None:
    """Fail loudly if SNAPI is ignoring our date filters.

    SNAPI returns 200 and the UNFILTERED set for an unrecognised parameter, so
    a renamed or removed filter would not raise — the backfill would just fetch
    everything into every window, and the load would look successful. This
    compares a deliberately narrow window against the unfiltered total: if
    filtering works the narrow count must be strictly smaller.
    """
    total = _get(path, {"limit": 1})["count"]
    narrow = _get(path, {"limit": 1, "published_at_lte": "1901-01-01T00:00:00Z"})["count"]
    if total == 0:
        raise RuntimeError(f"SNAPI {path} reports zero rows; refusing to treat that as a load.")
    if narrow >= total:
        raise RuntimeError(
            f"SNAPI {path} ignored `published_at_lte` — a 1900-1901 window returned "
            f"{narrow} of {total} rows. The filter has been renamed or removed upstream; "
            "the windowed backfill cannot be trusted until this is fixed."
        )
    logger.info("SNAPI %s: filter contract OK (%d rows total).", path, total)


def _walk_window(
    path: str,
    field: str,
    start: datetime,
    end: datetime,
    depth: int = 0,
) -> Iterator[dict]:
    """Yield every row with `start <= field < end`, splitting until it fits.

    Recursively halves the window while its `count` exceeds one page, so every
    actual fetch is a single request returning a complete window. That is what
    removes offset pagination — and with it SNAPI's unstable tie-ordering —
    from the correctness argument entirely.

    The bounds are half-open. SNAPI's `_lte` is inclusive, so the upper bound is
    nudged back by a second; a row exactly on a split boundary therefore lands
    in exactly one window rather than in both or neither.
    """
    params = {
        "limit": PAGE_LIMIT,
        f"{field}_gte": _iso(start),
        f"{field}_lte": _iso(end - timedelta(seconds=1)),
    }
    payload = _get(path, params)
    count = payload["count"]

    if count == 0:
        return

    if count <= PAGE_LIMIT:
        rows = payload["results"]
        if len(rows) != count:
            # One request was supposed to be the whole window.
            logger.warning(
                "SNAPI %s window %s..%s reported %d rows but returned %d.",
                path, _iso(start), _iso(end), count, len(rows),
            )
        yield from rows
        return

    span = end - start
    if span <= MIN_WINDOW:
        # Cannot split further. Fall back to offset paging and say so — this is
        # the one place the unstable-ordering risk reappears, so it must be
        # visible in the log rather than silently accepted.
        logger.warning(
            "SNAPI %s: %d rows inside a %s window at %s, which cannot be split "
            "further. Falling back to offset paging; rows may be missed if the "
            "upstream ordering is unstable.",
            path, count, span, _iso(start),
        )
        for offset in range(0, count, PAGE_LIMIT):
            yield from _get(path, {**params, "offset": offset})["results"]
        return

    midpoint = start + span / 2
    logger.debug(
        "SNAPI %s: splitting %s..%s (%d rows, depth %d).",
        path, _iso(start), _iso(end), count, depth,
    )
    yield from _walk_window(path, field, start, midpoint, depth + 1)
    yield from _walk_window(path, field, midpoint, end, depth + 1)


def _make_resource(resource_name: str, path: str, backfill: bool):
    """Build one dlt resource over a SNAPI collection.

    All three collections have the same envelope, the same filters and the same
    `id` key, so they differ only by path — hence one factory rather than three
    near-identical resource functions.
    """

    @dlt.resource(
        name=resource_name,
        write_disposition="merge",
        # `id` is SNAPI's own stable integer key and survives revisions, so a
        # revised article updates its row instead of adding one. Nothing
        # version-like belongs in this key.
        primary_key="id",
    )
    def collection() -> Iterator[dict]:
        state = dlt.current.resource_state()
        run_start = datetime.now(timezone.utc)

        if backfill:
            _check_filter_contract(path)
            field, start, end = "published_at", BACKFILL_START, run_start
            logger.info("SNAPI %s: FULL BACKFILL over published_at.", path)
        else:
            cursor = state.get("cursor")
            if cursor is None:
                # Never loaded. Fall back to a backfill rather than silently
                # loading only the last few hours and looking complete.
                _check_filter_contract(path)
                field, start, end = "published_at", BACKFILL_START, run_start
                logger.info(
                    "SNAPI %s: no cursor in state, so this run does a full backfill.", path
                )
            else:
                field = "updated_at"
                start = datetime.fromisoformat(cursor)
                end = run_start
                logger.info("SNAPI %s: incremental over updated_at since %s.", path, cursor)

        total = 0
        for row in _walk_window(path, field, start, end):
            total += 1
            yield row

        # Advance the cursor ONLY after the walk has completed. An exception
        # mid-walk leaves the old cursor in place, so the next run re-reads the
        # whole range rather than skipping whatever was missed.
        #
        # Clock-derived, not data-derived: everything that existed at run_start
        # has now been ingested, which is exactly what the cursor should mean.
        state["cursor"] = (run_start - CURSOR_LAG).isoformat()
        logger.info(
            "SNAPI %s: %d rows; cursor now %s.", path, total, state["cursor"]
        )

    return collection


@dlt.source(name="spaceflight_news")
def spaceflight_news_source(backfill: bool = False) -> Any:
    """Articles, blogs and reports from the Spaceflight News API.

    Args:
        backfill: Walk the whole archive by published_at instead of reading
            forward from the stored cursor. Safe to run repeatedly — the walk
            is deterministic and the merge key makes re-loading a no-op — but
            it costs a few hundred requests, so routine runs leave it False.
            A resource with no cursor in state backfills anyway, so the FIRST
            run is a full backfill whether or not this is set.
    """
    return [_make_resource(name, path, backfill) for name, path in SNAPI_RESOURCES]


def load_spaceflight_news(
    credentials: Optional[dict] = None,
    backfill: bool = False,
    dev_mode: bool = False,
    destination_override: Optional[Any] = None,
) -> str:
    """Load SNAPI into the Postgres schema `raw`. Returns load info.

    Args:
        credentials: Postgres connection as a dict of database/username/
            password/host/port. Omit to fall back to secrets.toml. The Airflow
            DAG builds this from the `norion-analytics-pg` Connection.
        backfill: Force a full archive walk. See the source docstring.
        dev_mode: Load into a fresh timestamped dataset instead of `raw`.
            Leave False for anything Airflow calls — dev datasets are invisible
            to dbt.
        destination_override: A dlt destination to use instead of Postgres, for
            smoke testing without warehouse credentials. Airflow never passes it.
    """
    if destination_override is not None:
        destination: Any = destination_override
    elif credentials:
        destination = dlt.destinations.postgres(credentials=credentials)
    else:
        destination = "postgres"

    pipeline = dlt.pipeline(
        pipeline_name="spaceflight_news",
        destination=destination,
        dataset_name="raw",
        pipelines_dir=_state_dir(),
        dev_mode=dev_mode,
    )

    info = pipeline.run(spaceflight_news_source(backfill=backfill))
    return str(info)


if __name__ == "__main__":
    # Smoke test: isolated dataset written as local files, so it needs no
    # warehouse credentials and leaves `raw` untouched.
    logging.basicConfig(level=logging.INFO)
    print(  # noqa: T201
        load_spaceflight_news(
            dev_mode=True,
            destination_override=dlt.destinations.filesystem(
                bucket_url="file:///tmp/snapi_smoke"
            ),
        )
    )
