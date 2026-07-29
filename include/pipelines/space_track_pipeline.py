"""Space-Track GP catalogue (and CDMs, if permitted) -> Postgres schema `raw`.

Space-Track is the authoritative public catalogue, run by US Space Command. It
is the only single source for the COMPLETE screening universe: CelesTrak's
GROUP=active is active payloads only (~16.2k), while this returns every
on-orbit object including debris (~28k+), which is the majority of objects and
the majority of collision risk.

Base: https://www.space-track.org/basicspacedata/query/
Docs: https://www.space-track.org/documentation

DELIBERATE DEVIATION from the onboard-rest-api-source recipe.
-----------------------------------------------------------
Every other source here is built with `rest_api_resources` and a declarative
RESTAPIConfig. This one is not, because Space-Track authenticates by COOKIE
SESSION: you POST credentials to /ajaxauth/login and reuse the resulting session
cookie on subsequent GETs. dlt's rest_api auth types cover api-key, bearer,
basic and OAuth2 — none of them model a login round-trip that sets a cookie.

So the resources below are plain @dlt.resource generators over a
requests.Session. Everything else about this module follows the recipe: same
file location, same naming, same dataset, same state directory, same
credentials-as-parameters contract. Only the extraction mechanism differs.

RATE LIMITS are strict and enforced: fewer than 30 requests per minute and 300
per hour. Space-Track's own guidance also asks for GP no more than hourly and
CDM no more than every 8 hours. This module makes TWO requests per run in total
— one bulk query per class, never one per object — so it is nowhere near the
ceiling. Do not "optimise" this into per-object queries; that is the documented
way to get an account suspended.

CDM ACCESS IS NOT GUARANTEED.
-----------------------------
Space-Track's documentation states that CDMs are served through advanced
services requiring an SSA Sharing Agreement or an Orbital Data Request, i.e.
they are generally available to spacecraft OWNER/OPERATORS rather than to any
registered account. This module therefore treats the CDM resource as OPTIONAL:
if the query returns 403/404 or an empty result, it logs and yields nothing
rather than failing the load. A missing CDM table is an expected outcome for a
non-operator account, not a bug.

That matters for how the output is read. Without CDMs there is no authoritative
probability of collision to reconcile against, and everything this platform
produces about collision risk is an ESTIMATE derived from TLE scatter. See
include/pipelines/conjunction_screening_pipeline.py.

Credentials are passed in by the caller. In Airflow that is the DAG, which
reads the `SPACE_TRACK_USER` and `SPACE_TRACK_PASSWORD` Variables and the
`norion-analytics-pg` Connection. When omitted, dlt falls back to
.dlt/secrets.toml under [sources.space_track], which exists only for running
this module by hand.

This module deliberately imports nothing from Airflow, so it stays runnable and
testable outside the scheduler.
"""

from __future__ import annotations

import logging
import time
from pathlib import Path
from typing import Any, Iterator, Optional

import dlt
import requests

logger = logging.getLogger(__name__)

BASE_URL = "https://www.space-track.org"
LOGIN_PATH = "/ajaxauth/login"
QUERY_PATH = "/basicspacedata/query"

# Space-Track asks callers to identify themselves.
USER_AGENT = "norion-warehouse/1.0 (+trevor.barnes91@gmail.com)"

# Seconds to wait between queries. The documented ceiling is 30/min, and this
# module issues two requests per run, so this is courtesy rather than
# necessity — it costs nothing and keeps us visibly well-behaved.
REQUEST_SPACING_SECONDS = 3.0

# How long a single query may take. The full-catalogue GP query returns tens of
# thousands of rows and Space-Track is not fast; the default requests timeout of
# None would hang a DAG task forever if the service stalled.
REQUEST_TIMEOUT_SECONDS = 300

# The full on-orbit catalogue: everything that has not decayed, with an element
# set from the last 30 days.
#
# Deliberately NOT filtered with NORAD_CAT_ID/<100000. That filter appears in
# Space-Track's own examples to exclude Alpha-5 objects (catalogue numbers above
# 99,999), which matters when requesting `format/tle` because the TLE text
# format encodes them with a leading letter. We request JSON, where the id comes
# back as a plain number, so the filter would only throw away real objects that
# a full-catalogue screen needs.
GP_QUERY = (
    "/class/gp/decay_date/null-val/epoch/%3Enow-30/orderby/norad_cat_id/format/json"
)

# Upcoming conjunctions. Optional — see module docstring.
CDM_QUERY = "/class/cdm_public/TCA/%3Enow/orderby/TCA/format/json"


def _state_dir() -> str:
    """Where dlt keeps load history and working files.

    Must survive container restarts, so it goes on the bind mount rather than
    in the container's ephemeral home. Falls back to the repo copy when
    /opt/airflow is absent, which is what makes this runnable locally.
    """
    container_dir = Path("/opt/airflow/include/warehouse")
    base = container_dir if container_dir.is_dir() else Path(__file__).resolve().parents[1] / "warehouse"
    return str(base / ".dlt_pipelines")


def _login(identity: str, password: str) -> requests.Session:
    """Authenticate and return a session carrying the auth cookie.

    Raises on failure rather than returning an unauthenticated session, because
    Space-Track answers unauthenticated queries with a 200 and a login page
    rather than a 401 — silently yielding zero rows is the failure mode we are
    avoiding here.
    """
    session = requests.Session()
    session.headers.update({"User-Agent": USER_AGENT})

    response = session.post(
        f"{BASE_URL}{LOGIN_PATH}",
        data={"identity": identity, "password": password},
        timeout=60,
    )
    response.raise_for_status()

    # A failed login returns 200 with a body describing the failure, so status
    # alone proves nothing. The auth cookie is the real signal.
    if not session.cookies:
        raise RuntimeError(
            "Space-Track login did not set a session cookie. Check the "
            "SPACE_TRACK_USER / SPACE_TRACK_PASSWORD Airflow Variables. "
            "(The credential values are never logged.)"
        )

    logger.info("Space-Track login succeeded.")
    return session


def _query(session: requests.Session, query: str) -> list[dict]:
    """Run one Space-Track query and return its rows."""
    url = f"{BASE_URL}{QUERY_PATH}{query}"
    logger.info("Space-Track query: %s", query)

    response = session.get(url, timeout=REQUEST_TIMEOUT_SECONDS)
    response.raise_for_status()

    payload = response.json()
    if not isinstance(payload, list):
        # Space-Track returns a dict describing the problem when a query is
        # malformed or not permitted.
        raise RuntimeError(f"Unexpected Space-Track response shape: {type(payload).__name__}")

    return payload


@dlt.source(name="space_track")
def space_track_source(
    identity: str = dlt.secrets.value,
    password: str = dlt.secrets.value,
    include_cdm: bool = True,
) -> Any:
    """The full on-orbit GP catalogue, plus CDMs when the account may see them.

    Args:
        identity: Space-Track username. Auto-loaded from secrets.toml under
            [sources.space_track].
        password: Space-Track password. Auto-loaded the same way. Never logged.
        include_cdm: Attempt the CDM query. Leave True — the resource degrades
            to zero rows on a permission failure rather than erroring.
    """
    session = _login(identity, password)

    # TABLE NAMES ARE PREFIXED, unlike every other source here.
    #
    # All pipelines share one dlt dataset (`raw`), so table names must be unique
    # across sources. celestrak_pipeline already owns the unqualified `gp` and
    # `sup_gp`, and "gp" is far too generic a name for a shared namespace — two
    # sources both calling their table `gp` would silently merge incompatible
    # schemas into one table. Prefixing here is the cheaper fix: celestrak's
    # tables already hold data, and renaming them would require a reload that
    # CelesTrak's 2-hour refusal window can block at an arbitrary moment.
    @dlt.resource(
        name="space_track_gp",
        write_disposition="merge",
        # (object, epoch) rather than object alone, so element-set history
        # accumulates for the pseudo-covariance estimate. Same rationale as
        # celestrak_pipeline — see its module docstring.
        primary_key=["NORAD_CAT_ID", "EPOCH"],
    )
    def gp() -> Iterator[dict]:
        rows = _query(session, GP_QUERY)
        logger.info("Space-Track GP returned %d objects.", len(rows))
        yield from rows

    @dlt.resource(
        name="space_track_cdm_public",
        write_disposition="merge",
        primary_key="CDM_ID",
    )
    def cdm_public() -> Iterator[dict]:
        time.sleep(REQUEST_SPACING_SECONDS)
        try:
            rows = _query(session, CDM_QUERY)
        except (requests.HTTPError, RuntimeError) as exc:
            # Expected for an account without an SSA Sharing Agreement or ODR.
            # Yield nothing rather than failing the whole load: the GP catalogue
            # is the part the screening engine actually needs.
            logger.warning(
                "Space-Track CDM query unavailable (%s). This is expected for a "
                "registered non-operator account — CDMs need an SSA Sharing "
                "Agreement or an Orbital Data Request. Continuing without them, "
                "which means no authoritative Pc is available to reconcile "
                "against.",
                exc,
            )
            return
        logger.info("Space-Track CDM returned %d messages.", len(rows))
        yield from rows

    resources = [gp]
    if include_cdm:
        resources.append(cdm_public)
    return resources


def load_space_track(
    identity: Optional[str] = None,
    password: Optional[str] = None,
    credentials: Optional[dict] = None,
    include_cdm: bool = True,
    dev_mode: bool = False,
    destination_override: Optional[Any] = None,
) -> str:
    """Load the Space-Track catalogue into the Postgres schema `raw`.

    Args:
        identity: Space-Track username. Omit to fall back to secrets.toml.
        password: Space-Track password. Omit to fall back to secrets.toml.
        credentials: Postgres connection as a dict of database/username/
            password/host/port. Omit to fall back to secrets.toml. The Airflow
            DAG builds this from the `norion-analytics-pg` Connection.
        include_cdm: Attempt the CDM resource.
        dev_mode: Load into a fresh timestamped dataset instead of `raw`.
        destination_override: A dlt destination to use instead of Postgres, for
            smoke testing without warehouse credentials. Airflow never passes it.

    Returns:
        The dlt load info, stringified.
    """
    # Omitted args must stay *absent* rather than None, or an explicit None
    # would override dlt's secrets.toml resolution instead of deferring to it.
    source_kwargs: dict = {"include_cdm": include_cdm}
    if identity is not None:
        source_kwargs["identity"] = identity
    if password is not None:
        source_kwargs["password"] = password

    if destination_override is not None:
        destination: Any = destination_override
    elif credentials:
        destination = dlt.destinations.postgres(credentials=credentials)
    else:
        destination = "postgres"

    pipeline = dlt.pipeline(
        pipeline_name="space_track",
        destination=destination,
        dataset_name="raw",
        pipelines_dir=_state_dir(),
        dev_mode=dev_mode,
    )

    info = pipeline.run(space_track_source(**source_kwargs))
    return str(info)


if __name__ == "__main__":
    # Smoke test: isolated dataset, written as local files, credentials from
    # .dlt/secrets.toml. Leaves `raw` untouched.
    logging.basicConfig(level=logging.INFO)
    print(  # noqa: T201
        load_space_track(
            dev_mode=True,
            destination_override=dlt.destinations.filesystem(
                bucket_url="file:///tmp/space_track_smoke"
            ),
        )
    )
