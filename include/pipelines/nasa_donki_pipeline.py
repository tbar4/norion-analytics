"""NASA DONKI space weather -> Postgres schema `raw`.

DONKI is the Space Weather Database Of Notifications, Knowledge, Information,
run by NASA's Space Weather Research Center.

Base: https://api.nasa.gov/DONKI/
Docs: https://api.nasa.gov/ (DONKI section)
      https://ccmc.gsfc.nasa.gov/tools/DONKI/

All eleven DONKI component services are loaded by this ONE source. They share
an auth scheme, a date-window contract and a response shape, so they are
resources of a single pipeline rather than eleven pipelines — one DAG, one dbt
tag, one state directory.

Shape, verified against the live API 2026-07-29 over a 2024-01-01..2024-03-31
window:

  resource                 path                  primary key                  rows/quarter
  cme                      CME                   activityID                            296
  cme_analysis             CMEAnalysis           associatedCMEID + time21_5            264
  gst                      GST                   gstID                                   3
  ips                      IPS                   activityID                             30
  flr                      FLR                   flrID                                 177
  sep                      SEP                   sepID                                  36
  mpc                      MPC                   mpcID                                   3
  rbe                      RBE                   rbeID                                   1
  hss                      HSS                   hssID                                   8
  wsa_enlil_simulation     WSAEnlilSimulations   simulationID                          212
  notification             notifications         messageID                              66

Every response is a bare JSON array with no envelope, hence
`data_selector: "$"`, and none of them paginate — the startDate/endDate window
*is* the volume control. There is no page size to tune.

WINDOW WIDTH IS THE WHOLE BALLGAME. DONKI has no `date=` parameter — startDate
and endDate are the only controls — and its response time scales with the width
of the window. Measured against the live API on 2026-08-02:

    resource        window      time    rows
    CME             1 day       0.4s       0
    CME             7 days      1.5s      29
    CME            30 days      5.9s     145
    CME           365 days     55.1s    1818   <- 503s whenever NASA is loaded

This DAG used a 365-day window, which is the entire explanation for the 503s in
the nasa_donki logs. The query is well-formed and does return 200 on a good
day; it is just slow enough to sit on the gateway's timeout, so it fails
whenever the upstream is under any load. CME is the resource that trips first
because it is by far the largest.

The window is now anchored to the RUN'S OWN DATA INTERVAL rather than to
wall-clock now, which is what makes a run idempotent: re-running an interval
requests the same dates and merges over its own previous output.

It is a short window rather than a single day, deliberately. Two properties of
DONKI make a strict one-day window lossy:

  * Events accumulate through the day. A run at 07:00 that asked only for its
    own date would capture nothing published later that day, and under a
    one-day-per-run schedule nothing would ever go back for the remainder.
  * Events are REVISED after publication (`versionId`, `submissionTime`). A
    revision does not move the event's date, so only a window that re-covers
    that date picks it up.

`LOOKBACK_DAYS` days of overlap fixes both, costs about 1.5 seconds, and stays
idempotent because the window is a pure function of the interval, not of when
the run happened to execute. Set it to 0 for a strict single-day window.

Two deliberate differences from nasa_apod:

  * `write_disposition` is "merge", not "replace". DONKI records carry
    `versionId` and `submissionTime` and are revised after publication, and the
    archive is cumulative rather than one-row-per-day. Replacing on a trailing
    window would discard every event older than the window on every run.

  * Most events carry nested arrays — instruments, linkedEvents,
    sentNotifications, cmeAnalyses, allKpIndex, impactList. dlt normalises
    those into child tables (`cme__instruments`, and so on), so this source
    produces roughly forty tables in `raw`, not eleven. That is expected.

Note on primary keys: `versionId` is deliberately NOT part of any key. It
changes when NASA revises an event, and including it would make each revision a
new row instead of an update — the opposite of what merge is for.

Credentials are passed in by the caller. In Airflow that is the DAG, which
reads the `NASA_API_KEY` Variable and the `norion-analytics-pg` Connection —
Airflow is the source of truth. `NASA_API_KEY` is an Airflow **Variable**, not
an environment variable; it is not in .env and not in the container env. When
either argument is omitted, dlt falls back to .dlt/secrets.toml, which is only
there for running this module by hand.

This module deliberately imports nothing from Airflow, so it stays runnable and
testable outside the scheduler.

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

# Days of overlap a routine run re-covers behind its own date, so that events
# published late in a day and events revised after publication both land. See
# the module docstring. 0 gives a strict single-day window.
LOOKBACK_DAYS = 7

# Widest window issued in one request when backfilling. 30 days answers in
# about 6 seconds and 365 sits on the gateway timeout, so this leaves room for
# a slow day upstream without making a long backfill excessively chatty.
CHUNK_DAYS = 30

# resource name -> (path, primary key). The single place this mapping lives;
# adding a DONKI service means adding one row here and nothing else.
DONKI_RESOURCES: list[tuple[str, str, Any]] = [
    ("cme", "CME", "activityID"),
    # CMEAnalysis has no id of its own. associatedCMEID alone is not unique
    # (236/264 — a CME gets several analyses); adding the 21.5-solar-radii
    # arrival time makes it unique (264/264). Verified 2026-07-29.
    ("cme_analysis", "CMEAnalysis", ["associatedCMEID", "time21_5"]),
    ("gst", "GST", "gstID"),
    ("ips", "IPS", "activityID"),
    ("flr", "FLR", "flrID"),
    ("sep", "SEP", "sepID"),
    ("mpc", "MPC", "mpcID"),
    ("rbe", "RBE", "rbeID"),
    ("hss", "HSS", "hssID"),
    ("wsa_enlil_simulation", "WSAEnlilSimulations", "simulationID"),
    ("notification", "notifications", "messageID"),
]


def _state_dir() -> str:
    """Where dlt keeps load history and working files.

    Must survive container restarts, so it goes on the bind mount rather than
    in the container's ephemeral home. Falls back to the repo copy when
    /opt/airflow is absent, which is what makes this runnable locally.
    """
    container_dir = Path("/opt/airflow/include/warehouse")
    base = container_dir if container_dir.is_dir() else Path(__file__).resolve().parents[1] / "warehouse"
    return str(base / ".dlt_pipelines")


@dlt.source(name="nasa_donki")
def nasa_donki_source(
    api_key: str = dlt.secrets.value,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    days_back: int = LOOKBACK_DAYS,
    base_url: str = "https://api.nasa.gov/DONKI/",
) -> Any:
    """Every DONKI space weather service over one bounded date window.

    Callers should keep the window under about 30 days — see the module
    docstring for why. `load_donki` slices longer backfills into `CHUNK_DAYS`
    chunks so that stays true without the caller having to think about it.

    Args:
        api_key: NASA API key. Auto-loaded from secrets.toml under
            [sources.nasa_donki]. Get a free one at https://api.nasa.gov/.
            DEMO_KEY works but is capped at 30 req/hr *shared across every
            anonymous caller*, so it rate-limits almost immediately — and this
            source makes eleven requests per run.
        start_date: First day to load, "YYYY-MM-DD". Defaults to `days_back`
            days before end_date.
        end_date: Last day to load, "YYYY-MM-DD". Defaults to today (UTC).
            Airflow passes the run's own interval here rather than letting this
            default fire, which is what makes a scheduled run reproducible.
        days_back: Window size used only when start_date is omitted.
        base_url: API root. Override only for testing against a mock.

    Examples:
        nasa_donki_source(end_date="2026-08-01")               # 7-day window
        nasa_donki_source(start_date="2026-08-01",
                          end_date="2026-08-01")               # single day
    """
    if end_date is None:
        end_date = pendulum.now("UTC").to_date_string()
    if start_date is None:
        start_date = pendulum.parse(end_date).subtract(days=days_back).to_date_string()

    resources: list[dict] = []
    for name, path, primary_key in DONKI_RESOURCES:
        endpoint: dict = {"path": path}
        # notifications is the only service that needs a param beyond the date
        # window; without type it returns nothing.
        if path == "notifications":
            endpoint["params"] = {"type": "all"}
        resources.append({"name": name, "primary_key": primary_key, "endpoint": endpoint})

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
        # Every DONKI service behaves identically, so the shared contract lives
        # here once rather than being repeated eleven times.
        "resource_defaults": {
            "write_disposition": "merge",
            "endpoint": {
                # Bare JSON array at the root, no envelope.
                "data_selector": "$",
                # No pagination: the whole range arrives in one response.
                "paginator": {"type": "single_page"},
                "params": {"startDate": start_date, "endDate": end_date},
            },
        },
        "resources": resources,
    }

    yield from rest_api_resources(config)


def load_donki(
    api_key: Optional[str] = None,
    credentials: Optional[dict] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    days_back: int = LOOKBACK_DAYS,
    dev_mode: bool = False,
) -> str:
    """Load every DONKI service into the Postgres schema `raw`. Returns load info.

    Any window wider than `CHUNK_DAYS` is SLICED and run as one load per chunk.
    That slicing is the point: a single 365-day request takes 55 seconds and
    503s whenever NASA is under load (see the module docstring). Every mode
    merges on each event's natural key, so overlapping chunks and re-runs
    converge on the same table rather than duplicating rows.

    Args:
        api_key: NASA key. Omit to fall back to secrets.toml (local runs).
        credentials: Postgres connection as a dict of database/username/
            password/host/port. Omit to fall back to secrets.toml. The Airflow
            DAG builds this from the `norion-analytics-pg` Connection.
        start_date: First day to load. Defaults to `days_back` before end_date.
        end_date: Last day to load. Defaults to today (UTC).
        days_back: Window size used only when start_date is omitted.
        dev_mode: Load into a fresh timestamped dataset instead of `raw`.
            Useful for iterating, but invisible to anything reading `raw`, so
            leave it False for anything Airflow calls.
    """
    destination = dlt.destinations.postgres(credentials=credentials) if credentials else "postgres"

    pipeline = dlt.pipeline(
        pipeline_name="nasa_donki",
        destination=destination,
        dataset_name="raw",
        pipelines_dir=_state_dir(),
        dev_mode=dev_mode,
    )

    last = pendulum.parse(end_date) if end_date else pendulum.now("UTC")
    first = pendulum.parse(start_date) if start_date else last.subtract(days=days_back)

    chunks = max(1, -(-((last - first).days + 1) // CHUNK_DAYS))
    if chunks > 1:
        # Every chunk costs one request PER RESOURCE, and there are eleven of
        # them. A backfill to DONKI's 2010 archive is roughly 195 chunks, i.e.
        # ~2,100 requests — over NASA's 1,000/hour limit, which answers with 429
        # rather than failing loudly. Say so up front instead of letting a long
        # backfill quietly degrade halfway through.
        logger.info(
            "DONKI backfill %s..%s: %d chunks x %d resources = ~%d requests. "
            "NASA's limit is 1,000/hour; split the range if this exceeds it.",
            first.to_date_string(),
            last.to_date_string(),
            chunks,
            len(DONKI_RESOURCES),
            chunks * len(DONKI_RESOURCES),
        )

    infos: list[str] = []
    cursor = first
    while cursor <= last:
        chunk_end = min(cursor.add(days=CHUNK_DAYS - 1), last)
        # Omitted args must stay *absent* rather than None, or an explicit None
        # would override dlt's secrets.toml resolution instead of deferring.
        source_kwargs: dict = {
            "start_date": cursor.to_date_string(),
            "end_date": chunk_end.to_date_string(),
        }
        if api_key is not None:
            source_kwargs["api_key"] = api_key

        logger.info(
            "DONKI window %s..%s", cursor.to_date_string(), chunk_end.to_date_string()
        )
        infos.append(str(pipeline.run(nasa_donki_source(**source_kwargs))))
        cursor = chunk_end.add(days=1)

    if len(infos) > 1:
        logger.info("DONKI backfill complete: %d chunks.", len(infos))
    return "\n".join(infos)


if __name__ == "__main__":
    # Smoke test: small window, isolated dataset, leaves `raw` untouched.
    print(load_donki(days_back=7, dev_mode=True))  # noqa: T201
