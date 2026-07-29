"""NASA Astronomy Picture of the Day (APOD) -> Postgres schema `raw`.

Single endpoint: GET https://api.nasa.gov/planetary/apod

The API has no pagination. Passing start_date/end_date returns the whole
range as one bare JSON array, which is why the resource uses
`data_selector: "$"` and the `single_page` paginator. The date window *is*
the volume control here — there is no page size to tune.

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

from pathlib import Path
from typing import Any, Optional

import dlt
from dlt.common.pendulum import pendulum
from dlt.sources.rest_api import RESTAPIConfig, rest_api_resources

# APOD's own archive starts here. The API 400s on earlier dates.
APOD_EPOCH = "1995-06-16"


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
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    days_back: int = 365,
    base_url: str = "https://api.nasa.gov/planetary/",
) -> Any:
    """Astronomy Picture of the Day over a date range.

    Args:
        api_key: NASA API key. Auto-loaded from secrets.toml under
            [sources.nasa_apod]. Get a free one at https://api.nasa.gov/.
            DEMO_KEY works but is capped at 30 req/hr and 50/day.
        start_date: First day to load, "YYYY-MM-DD". Defaults to
            `days_back` days before end_date. Clamped to 1995-06-16.
        end_date: Last day to load, "YYYY-MM-DD". Defaults to today (UTC).
        days_back: Window size used only when start_date is omitted.
        base_url: API root. Override only for testing against a mock.

    Examples:
        nasa_apod_source()                          # trailing year
        nasa_apod_source(days_back=7)               # smoke test
        nasa_apod_source(start_date=APOD_EPOCH)     # full archive
    """
    if end_date is None:
        end_date = pendulum.now("UTC").to_date_string()
    if start_date is None:
        start_date = pendulum.parse(end_date).subtract(days=days_back).to_date_string()
    if start_date < APOD_EPOCH:
        start_date = APOD_EPOCH

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
                "primary_key": "date",
                "write_disposition": "replace",
                "endpoint": {
                    "path": "apod",
                    # Bare JSON array at the root, no envelope.
                    "data_selector": "$",
                    # No pagination: the whole range arrives in one response.
                    "paginator": {"type": "single_page"},
                    "params": {
                        "start_date": start_date,
                        "end_date": end_date,
                        "thumbs": True,
                    },
                },
            },
        ],
    }

    yield from rest_api_resources(config)


def load_apod(
    api_key: Optional[str] = None,
    credentials: Optional[dict] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    days_back: int = 365,
    dev_mode: bool = False,
) -> str:
    """Load APOD into the Postgres schema `raw`. Returns load info.

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
        pipeline_name="nasa_apod",
        destination=destination,
        dataset_name="raw",
        pipelines_dir=_state_dir(),
        dev_mode=dev_mode,
    )

    info = pipeline.run(nasa_apod_source(**source_kwargs))
    return str(info)


if __name__ == "__main__":
    # Smoke test: small window, isolated dataset, leaves `raw` untouched.
    print(load_apod(days_back=7, dev_mode=True))  # noqa: T201
