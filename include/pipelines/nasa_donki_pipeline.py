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

from pathlib import Path
from typing import Any, Optional

import dlt
from dlt.common.pendulum import pendulum
from dlt.sources.rest_api import RESTAPIConfig, rest_api_resources

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
    days_back: int = 30,
    base_url: str = "https://api.nasa.gov/DONKI/",
) -> Any:
    """Every DONKI space weather service over a date range.

    Args:
        api_key: NASA API key. Auto-loaded from secrets.toml under
            [sources.nasa_donki]. Get a free one at https://api.nasa.gov/.
            DEMO_KEY works but is capped at 30 req/hr *shared across every
            anonymous caller*, so it rate-limits almost immediately — and this
            source makes eleven requests per run.
        start_date: First day to load, "YYYY-MM-DD". Defaults to `days_back`
            days before end_date. Pass an early date to backfill the archive.
        end_date: Last day to load, "YYYY-MM-DD". Defaults to today (UTC).
        days_back: Window size used only when start_date is omitted. DONKI's
            own default is 30 days; kept here so a routine run stays small
            while merge accumulates history across runs.
        base_url: API root. Override only for testing against a mock.

    Examples:
        nasa_donki_source()                            # trailing 30 days
        nasa_donki_source(days_back=7)                 # smoke test
        nasa_donki_source(start_date="2010-01-01")     # backfill the archive
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
    days_back: int = 30,
    dev_mode: bool = False,
) -> str:
    """Load every DONKI service into the Postgres schema `raw`. Returns load info.

    Args:
        api_key: NASA key. Omit to fall back to secrets.toml (local runs).
        credentials: Postgres connection as a dict of database/username/
            password/host/port. Omit to fall back to secrets.toml. The Airflow
            DAG builds this from the `norion-analytics-pg` Connection.
        dev_mode: Load into a fresh timestamped dataset instead of `raw`.
            Useful for iterating, but invisible to anything reading `raw`, so
            leave it False for anything Airflow calls.
    """
    # Omitted args must stay *absent* rather than None, or an explicit None
    # would override dlt's secrets.toml resolution instead of deferring to it.
    source_kwargs = {"start_date": start_date, "end_date": end_date, "days_back": days_back}
    if api_key is not None:
        source_kwargs["api_key"] = api_key

    destination = dlt.destinations.postgres(credentials=credentials) if credentials else "postgres"

    pipeline = dlt.pipeline(
        pipeline_name="nasa_donki",
        destination=destination,
        dataset_name="raw",
        pipelines_dir=_state_dir(),
        dev_mode=dev_mode,
    )

    info = pipeline.run(nasa_donki_source(**source_kwargs))
    return str(info)


if __name__ == "__main__":
    # Smoke test: small window, isolated dataset, leaves `raw` untouched.
    print(load_donki(days_back=7, dev_mode=True))  # noqa: T201
