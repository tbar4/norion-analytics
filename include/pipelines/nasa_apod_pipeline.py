"""NASA Astronomy Picture of the Day (APOD) -> Postgres schema `raw`.

Single endpoint: GET https://api.nasa.gov/planetary/apod

ONE DAY PER RUN. A scheduled run asks for `date=<the run's own day>` and
loads exactly that day's entry, which is what makes the DAG idempotent:
re-running an interval re-fetches the same day and merges over its own
previous row instead of producing something different.

Why not a trailing range, which is what this did until 2026-08-02:

  api.nasa.gov has a hard latency ceiling somewhere under 30 seconds, and the
  response time scales with the width of the window. Measured against the live
  API on 2026-08-02:

    date=<one day>          0.2s    200
    start/end,   7 days     0.8s    200
    start/end,  30 days     1.9s    200
    start/end,  90 days     6.5s    200
    start/end, 365 days    30.3s    500   <- always

  The 365-day window this DAG used was not a malformed query — it was simply
  too slow, and the gateway turned it into an Internal Server Error. That is
  the entire explanation for the 500s in the nasa_apod logs, and no amount of
  retrying fixes it because the failure is deterministic.

`date=` and the range form return DIFFERENT SHAPES: `date=` returns a bare
JSON object, the range form a bare JSON array. Both are unwrapped, hence
`data_selector: "$"` and the `single_page` paginator in both modes — dlt
treats a selected object as a single record.

Range mode is kept for backfilling the archive, but the range is sliced into
`CHUNK_DAYS`-day requests (see `load_apod`) so a backfill can never reissue
the window that fails.

WRITE DISPOSITION IS "merge", NOT "replace". That is load-bearing now that a
run fetches one day: under `replace` each daily run would truncate the table
and leave a single row. Merge on the natural key `date` means a run adds its
day and corrects it if NASA revised it.

Two fields are conditionally absent: `copyright` (many images are public
domain) and `hdurl` (video entries have no HD still). dlt models those as
nullable columns, so NULLs there are expected, not a load failure.

Credentials are passed in by the caller. In Airflow that is the DAG, which
reads the `NASA_API_KEY` Variable and the `norion-analytics-pg` Connection —
Airflow is the source of truth. When either argument is omitted, dlt falls
back to .dlt/secrets.toml, which is only there for running this module by
hand on the workstation.

This module deliberately imports nothing from Airflow, so it stays runnable
and testable outside the scheduler.

Lives in include/pipelines/ rather than include/dlt/ on purpose: PYTHONPATH
includes /opt/airflow/include and is searched before site-packages, so a
directory named dlt/ would shadow the installed dlt library.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, Optional

import dlt
from dlt.common.pendulum import pendulum
from dlt.sources.rest_api import RESTAPIConfig, rest_api_resources

logger = logging.getLogger(__name__)

# APOD's own archive starts here. The API 400s on earlier dates.
APOD_EPOCH = "1995-06-16"

# Widest range issued in one request when backfilling. 90 days still answers in
# 6.5s and 365 always 500s, so 30 leaves a wide margin for a slow day upstream
# without making a long backfill unreasonably chatty.
CHUNK_DAYS = 30


def _state_dir() -> str:
    """Where dlt keeps load history and working files.

    Must survive container restarts, so it goes on the bind mount rather than
    in the container's ephemeral home. Falls back to the repo copy when
    /opt/airflow is absent, which is what makes this runnable locally.
    """
    container_dir = Path("/opt/airflow/include/warehouse")
    base = container_dir if container_dir.is_dir() else Path(__file__).resolve().parents[1] / "warehouse"
    return str(base / ".dlt_pipelines")


@dlt.source(name="nasa_apod")
def nasa_apod_source(
    api_key: str = dlt.secrets.value,
    date: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    base_url: str = "https://api.nasa.gov/planetary/",
) -> Any:
    """Astronomy Picture of the Day, for one day or one bounded range.

    Exactly one of the two modes applies:

      * `date` set          -> `?date=D`, one entry. What a scheduled run uses.
      * `start_date`/`end_date` -> `?start_date=..&end_date=..`, the range form.
        Callers should keep the range under ~90 days; `load_apod` slices longer
        backfills into `CHUNK_DAYS` chunks so this stays true.

    Args:
        api_key: NASA API key. Auto-loaded from secrets.toml under
            [sources.nasa_apod]. Get a free one at https://api.nasa.gov/.
            DEMO_KEY works but is capped at 30 req/hr and 50/day.
        date: The single day to load, "YYYY-MM-DD". Takes precedence over the
            range arguments. A day outside 1995-06-16..today makes the API
            return 400 with an explicit "Date must be between" message.
        start_date: First day of a range load. Clamped to 1995-06-16.
        end_date: Last day of a range load. Defaults to today (UTC).
        base_url: API root. Override only for testing against a mock.

    Examples:
        nasa_apod_source(date="2026-08-01")                      # one day
        nasa_apod_source(start_date="2026-07-01", end_date="2026-07-30")
    """
    if date is not None:
        # Single-day form. Returns a bare JSON OBJECT, not an array.
        params: dict = {"date": date, "thumbs": True}
    else:
        if end_date is None:
            end_date = pendulum.now("UTC").to_date_string()
        if start_date is None:
            start_date = pendulum.parse(end_date).subtract(days=CHUNK_DAYS).to_date_string()
        if start_date < APOD_EPOCH:
            start_date = APOD_EPOCH
        # Range form. Returns a bare JSON ARRAY.
        params = {"start_date": start_date, "end_date": end_date, "thumbs": True}

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
                "name": "apod",
                # The natural key. Merge rather than replace: a run now loads a
                # single day, and `replace` would truncate the table down to
                # that one row on every run.
                "primary_key": "date",
                "write_disposition": "merge",
                "endpoint": {
                    "path": "apod",
                    # No envelope in either mode: a bare object for `date=`, a
                    # bare array for the range form. "$" selects both, and dlt
                    # treats a selected object as one record.
                    "data_selector": "$",
                    # No pagination: the whole response arrives at once.
                    "paginator": {"type": "single_page"},
                    "params": params,
                },
            },
        ],
    }

    yield from rest_api_resources(config)


def load_apod(
    api_key: Optional[str] = None,
    credentials: Optional[dict] = None,
    date: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    dev_mode: bool = False,
) -> str:
    """Load APOD into the Postgres schema `raw`. Returns load info.

    Two modes, mirroring `nasa_apod_source`:

      * `date` set -> one day, one request. What the Airflow DAG calls, with
        the day taken from the run's own data interval.
      * `start_date`/`end_date` -> a backfill, SLICED into `CHUNK_DAYS` chunks
        and run as one load per chunk. The slicing is the point: a single
        365-day request returns 500 every time (see the module docstring).

    Both modes merge on `date`, so re-running any day or any overlapping range
    converges on the same table rather than duplicating or truncating.

    Args:
        api_key: NASA key. Omit to fall back to secrets.toml (local runs).
        credentials: Postgres connection as a dict of database/username/
            password/host/port. Omit to fall back to secrets.toml. The Airflow
            DAG builds this from the `norion-analytics-pg` Connection.
        date: Single day to load, "YYYY-MM-DD".
        start_date: First day of a backfill. Clamped to 1995-06-16.
        end_date: Last day of a backfill. Defaults to today (UTC).
        dev_mode: Load into a fresh timestamped dataset instead of `raw`.
            Useful for iterating, but invisible to anything reading `raw`, so
            leave it False for anything Airflow calls.
    """
    destination = dlt.destinations.postgres(credentials=credentials) if credentials else "postgres"

    pipeline = dlt.pipeline(
        pipeline_name="nasa_apod",
        destination=destination,
        dataset_name="raw",
        pipelines_dir=_state_dir(),
        dev_mode=dev_mode,
    )

    def _run(**window: str) -> str:
        # Omitted args must stay *absent* rather than None, or an explicit None
        # would override dlt's secrets.toml resolution instead of deferring.
        source_kwargs: dict = dict(window)
        if api_key is not None:
            source_kwargs["api_key"] = api_key
        return str(pipeline.run(nasa_apod_source(**source_kwargs)))

    if date is not None:
        logger.info("Loading APOD for %s.", date)
        return _run(date=date)

    # Backfill. Resolve the window, then walk it in chunks.
    last = pendulum.parse(end_date) if end_date else pendulum.now("UTC")
    first = pendulum.parse(start_date) if start_date else last.subtract(days=CHUNK_DAYS)
    if first < pendulum.parse(APOD_EPOCH):
        first = pendulum.parse(APOD_EPOCH)

    infos: list[str] = []
    cursor = first
    while cursor <= last:
        chunk_end = min(cursor.add(days=CHUNK_DAYS - 1), last)
        logger.info(
            "APOD backfill chunk %s..%s", cursor.to_date_string(), chunk_end.to_date_string()
        )
        infos.append(
            _run(start_date=cursor.to_date_string(), end_date=chunk_end.to_date_string())
        )
        cursor = chunk_end.add(days=1)

    logger.info("APOD backfill complete: %d chunk(s).", len(infos))
    return "\n".join(infos)


if __name__ == "__main__":
    # Smoke test: one day, isolated dataset, leaves `raw` untouched.
    print(load_apod(date=pendulum.now("UTC").to_date_string(), dev_mode=True))  # noqa: T201
