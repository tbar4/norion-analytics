"""CelesTrak GP element sets -> Postgres schema `raw`.

CelesTrak (Dr T.S. Kelso) redistributes the public satellite catalogue as GP
(General Perturbations) element sets in OMM form. No account, no auth, no
rate-limit ceiling — which is what makes it the everyday feed here.

Base: https://celestrak.org/NORAD/elements/
Docs: https://celestrak.org/NORAD/documentation/gp-data-formats.php
Usage policy: https://celestrak.org/publications/

Two resources, and they are NOT the same kind of data:

  resource   endpoint        one row is                          rows (2026-07-29)
  gp         gp.php          an element set for a tracked object          16,236
  sup_gp     sup-gp.php      an OPERATOR-supplied element set             10,877 (starlink)

`gp` with GROUP=active is *active payloads only*. It is deliberately NOT the
full catalogue: debris is the majority of tracked objects and the majority of
collision risk, and CelesTrak does not expose the whole catalogue as one
endpoint. The complete screening universe comes from Space-Track's /class/gp/
instead — see include/pipelines/space_track_pipeline.py. Both land here and the
screening engine unions them, preferring the more accurate source per object.

`sup_gp` is supplemental data published by the operators themselves (SpaceX,
OneWeb, Planet). It is materially more accurate than the public TLE for exactly
the objects that dominate the catalogue by count, so it is worth having
separately rather than folded into `gp`.

Two traps in the supplemental feed, both verified live 2026-07-29:

  * NORAD_CAT_ID is a PLACEHOLDER. Starlink supplemental rows come back as
    100001 with CLASSIFICATION_TYPE "C" for objects that are not yet catalogued.
    Joining sup_gp to gp on norad_cat_id would collapse thousands of distinct
    satellites onto one bogus id. OBJECT_ID (the international designator,
    e.g. 2026-160A) is the stable key and is what this module keys on.
  * sup_gp carries an extra RMS column that gp does not, so the two tables have
    different shapes. That is why they are separate tables rather than one.

WRITE DISPOSITION — the important design decision here. Both resources use
`merge` keyed on (identifier, EPOCH) rather than on the identifier alone, so
each new element set for an object is a NEW ROW and history accumulates. That
is deliberate: the pseudo-covariance estimate in the screening engine works by
propagating the last N element sets for an object to a common epoch and taking
the scatter, which is only possible if past element sets were retained. Keying
on the identifier alone would overwrite yesterday's elements and destroy the
only uncertainty signal a TLE-based pipeline has.

CACHING — CelesTrak returns 403, not 304, when the data has not changed since
your last download. See CELESTRAK_NOT_MODIFIED_ACTION. GP data refreshes every
2 hours, so a run inside that window legitimately loads zero new rows; that is
success, not failure.

The cost of that is growth: roughly 16k objects x a few element sets a day.
Prune with a retention policy on raw.gp once the history is deeper than the
covariance window needs — it does not need to be kept forever.

Credentials: none. CelesTrak requires no key. The Postgres credentials are
passed in by the caller; in Airflow that is the DAG, reading the
`norion-analytics-pg` Connection.

This module deliberately imports nothing from Airflow, so it stays runnable and
testable outside the scheduler.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Callable, Optional

import dlt
from dlt.sources.rest_api import RESTAPIConfig, rest_api_resources

# CelesTrak asks callers to identify themselves rather than arrive anonymously.
# See their usage policy: cache, do not poll faster than the data changes, and
# be identifiable so they can contact you instead of blocking you.
USER_AGENT = "norion-warehouse/1.0 (+trevor.barnes91@gmail.com)"

# GP groups to pull. "active" is the broad one; the rest are here so a screening
# run can be pointed at a narrower, cheaper universe during development.
# Adding a group is a one-line change and costs one extra request per run.
GP_GROUPS: list[str] = ["active"]

# Operator-supplied supplemental element sets. These are the accuracy win.
SUP_GP_FILES: list[str] = ["starlink", "oneweb"]

# Explicit types for the OMM numeric fields.
#
# Without these dlt infers from the first rows it sees and gets it wrong: on the
# "stations" group every MEAN_MOTION_DDOT is 0, so dlt typed the column bigint,
# then forked a `mean_motion_ddot__v_double` VARIANT column the moment a real
# float arrived. Half the values in one column, half in another, silently.
#
# Every one of these is a float in the OMM spec even when a particular sample
# happens to contain only integers, so pin them all rather than only the one
# that bit. Verified against the stations group 2026-07-29.
# CelesTrak answers a repeat request for unchanged data with **403 Forbidden**,
# not 304 Not Modified. The body says so explicitly:
#
#   GP data has not updated since your last successful download of
#   GROUP=active at 2026-07-29 05:56:47 UTC. Data is updated once every
#   2 hours.
#
# That is server-side enforcement of their usage policy — a caching signal, not
# a failure, and not a ban. Without this action a routine run that happens to
# fall inside the 2-hour window fails the whole DAG. Verified live 2026-07-29:
# a probe at 05:56 caused the 06:04 DAG run to 403.
#
# Matching on the body text as well as the status is deliberate. A bare
# `status_code: 403 -> ignore` would also swallow a genuine block for abuse,
# which is exactly the signal we would want to see fail loudly.
CELESTRAK_NOT_MODIFIED_ACTION: list[dict[str, Any]] = [
    {"status_code": 403, "content": "has not updated", "action": "ignore"},
]

OMM_FLOAT_COLUMNS: dict[str, Any] = {
    "mean_motion": {"data_type": "double"},
    "eccentricity": {"data_type": "double"},
    "inclination": {"data_type": "double"},
    "ra_of_asc_node": {"data_type": "double"},
    "arg_of_pericenter": {"data_type": "double"},
    "mean_anomaly": {"data_type": "double"},
    "bstar": {"data_type": "double"},
    "mean_motion_dot": {"data_type": "double"},
    "mean_motion_ddot": {"data_type": "double"},
}


def _state_dir() -> str:
    """Where dlt keeps load history and working files.

    Must survive container restarts, so it goes on the bind mount rather than
    in the container's ephemeral home. Falls back to the repo copy when
    /opt/airflow is absent, which is what makes this runnable locally.
    """
    container_dir = Path("/opt/airflow/include/warehouse")
    base = container_dir if container_dir.is_dir() else Path(__file__).resolve().parents[1] / "warehouse"
    return str(base / ".dlt_pipelines")


def _tag(column: str, value: str) -> Callable[[dict], dict]:
    """Stamp a constant column onto every row of a resource.

    CelesTrak's responses do not say which group or supplemental file they came
    from, but a row is meaningless without that provenance once several groups
    share a table.
    """

    def _map(row: dict) -> dict:
        row[column] = value
        return row

    return _map


@dlt.source(name="celestrak")
def celestrak_source(
    gp_groups: Optional[list[str]] = None,
    sup_gp_files: Optional[list[str]] = None,
    base_url: str = "https://celestrak.org/NORAD/elements/",
) -> Any:
    """Public GP element sets plus operator supplemental element sets.

    Args:
        gp_groups: CelesTrak GROUP names to fetch into `gp`. Defaults to
            GP_GROUPS. Pass a narrow group such as ["stations"] for a cheap
            smoke test.
        sup_gp_files: Supplemental FILE names to fetch into `sup_gp`. Defaults
            to SUP_GP_FILES. Pass [] to skip the supplemental feed entirely.
        base_url: API root. Override only for testing against a mock.

    Examples:
        celestrak_source()                               # active + supplemental
        celestrak_source(gp_groups=["stations"],
                         sup_gp_files=[])                # smoke test, 23 rows
    """
    groups = GP_GROUPS if gp_groups is None else gp_groups
    files = SUP_GP_FILES if sup_gp_files is None else sup_gp_files

    resources: list[dict] = []

    for group in groups:
        resources.append(
            {
                "name": f"gp_{group.replace('-', '_')}",
                # Every group shares one table; the celestrak_group column
                # carries the provenance.
                "table_name": "gp",
                # (object, epoch) rather than object alone — see module docstring.
                "primary_key": ["NORAD_CAT_ID", "EPOCH"],
                "endpoint": {
                    "path": "gp.php",
                    "params": {"GROUP": group, "FORMAT": "json"},
                },
                "processing_steps": [{"map": _tag("celestrak_group", group)}],
            }
        )

    for file_name in files:
        resources.append(
            {
                "name": f"sup_gp_{file_name.replace('-', '_')}",
                "table_name": "sup_gp",
                # OBJECT_ID, never NORAD_CAT_ID — the supplemental feed returns
                # a placeholder catalogue number. See module docstring.
                "primary_key": ["OBJECT_ID", "EPOCH"],
                "endpoint": {
                    "path": "supplemental/sup-gp.php",
                    "params": {"FILE": file_name, "FORMAT": "json"},
                },
                "processing_steps": [{"map": _tag("celestrak_file", file_name)}],
            }
        )

    config: RESTAPIConfig = {
        "client": {
            "base_url": base_url,
            "headers": {"User-Agent": USER_AGENT},
        },
        "resource_defaults": {
            "write_disposition": "merge",
            # Pin the float types so dlt never forks a variant column. See
            # OMM_FLOAT_COLUMNS.
            "columns": OMM_FLOAT_COLUMNS,
            "endpoint": {
                # Bare JSON array at the root, no envelope.
                "data_selector": "$",
                # No pagination: the whole group arrives in one response.
                "paginator": {"type": "single_page"},
                "response_actions": CELESTRAK_NOT_MODIFIED_ACTION,
            },
        },
        "resources": resources,
    }

    yield from rest_api_resources(config)


def load_celestrak(
    credentials: Optional[dict] = None,
    gp_groups: Optional[list[str]] = None,
    sup_gp_files: Optional[list[str]] = None,
    dev_mode: bool = False,
    destination_override: Optional[Any] = None,
) -> str:
    """Load CelesTrak GP element sets into the Postgres schema `raw`.

    Args:
        credentials: Postgres connection as a dict of database/username/
            password/host/port. Omit to fall back to secrets.toml. The Airflow
            DAG builds this from the `norion-analytics-pg` Connection.
        gp_groups: Override the GP groups to fetch.
        sup_gp_files: Override the supplemental files to fetch.
        dev_mode: Load into a fresh timestamped dataset instead of `raw`.
            Useful for iterating, but invisible to anything reading `raw`, so
            leave it False for anything Airflow calls.
        destination_override: A dlt destination to use instead of Postgres.
            Exists so the standalone smoke test can verify extraction and
            normalisation without warehouse credentials — the local
            .dlt/secrets.toml Postgres fallback points at localhost, not at
            10.0.0.50. Airflow never passes this.

    Returns:
        The dlt load info, stringified.
    """
    if destination_override is not None:
        destination: Any = destination_override
    elif credentials:
        destination = dlt.destinations.postgres(credentials=credentials)
    else:
        destination = "postgres"

    pipeline = dlt.pipeline(
        pipeline_name="celestrak",
        destination=destination,
        dataset_name="raw",
        pipelines_dir=_state_dir(),
        dev_mode=dev_mode,
    )

    info = pipeline.run(celestrak_source(gp_groups=gp_groups, sup_gp_files=sup_gp_files))
    return str(info)


if __name__ == "__main__":
    # Smoke test: the 23-object "stations" group, no supplemental feed,
    # isolated dataset, written as local files. Verifies extraction and
    # normalisation without touching the warehouse or needing its credentials.
    #
    # Filesystem rather than DuckDB because the duckdb extra was dropped from
    # requirements.txt on 2026-07-28; `filesystem` is still an installed extra
    # and is enough to prove the shape of what would be loaded.
    print(  # noqa: T201
        load_celestrak(
            gp_groups=["stations"],
            sup_gp_files=[],
            dev_mode=True,
            destination_override=dlt.destinations.filesystem(
                bucket_url="file:///tmp/celestrak_smoke"
            ),
        )
    )
