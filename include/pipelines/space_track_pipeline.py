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
from datetime import datetime, timedelta, timezone
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

# RATE LIMITING — Space-Track enforces these, and exceeding them gets accounts
# suspended rather than merely throttled.
#
# Documented ceilings: fewer than 30 requests per minute and 300 per hour. The
# budgets below sit deliberately under both, because the ceiling is shared with
# anything else using the same account (a notebook, a manual query in the web
# UI) and arriving exactly at the limit leaves no room for that.
#
# The routine run makes two requests and never comes near this. The limiter
# exists for the gp_history BACKFILL, which issues one request per day of
# history and is the only thing here capable of getting the account into
# trouble.
MAX_REQUESTS_PER_MINUTE = 20
MAX_REQUESTS_PER_HOUR = 200

# Floor between any two requests, independent of the rolling windows above.
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

# Historical element sets, for backfill. Same predicates as the gp class; the
# database holds ~138 million elsets, so this is ALWAYS queried in bounded
# windows and never unfiltered.
#
# Chunked by CREATION_DATE — the field that records when Space-Track published
# an elset — rather than by EPOCH. The distinction matters: EPOCH is when the
# orbit solution is valid, CREATION_DATE is when it became knowable. Chunking on
# EPOCH would produce overlapping, unstable windows because a single publication
# can carry an epoch days earlier.
GP_HISTORY_QUERY = (
    "/class/gp_history/decay_date/null-val"
    "/CREATION_DATE/{start}--{end}"
    "/orderby/NORAD_CAT_ID,EPOCH/format/json"
)

# One request per day of backfill. A day of the full catalogue is on the order
# of tens of thousands of elsets, which Space-Track serves comfortably; a
# multi-day window in one request is what times out.
GP_HISTORY_CHUNK_DAYS = 1


class _RateLimiter:
    """Enforce Space-Track's per-minute and per-hour request ceilings.

    A fixed sleep between requests is not enough on its own: it satisfies the
    per-minute rule while still drifting past the hourly one over a long
    backfill. This tracks actual request timestamps and blocks until BOTH
    rolling windows have room.

    Not thread-safe, which is fine — the pipeline issues requests serially and
    should keep doing so. Parallelising them is precisely what the ceilings
    exist to prevent.
    """

    def __init__(
        self,
        per_minute: int = MAX_REQUESTS_PER_MINUTE,
        per_hour: int = MAX_REQUESTS_PER_HOUR,
        spacing_seconds: float = REQUEST_SPACING_SECONDS,
    ) -> None:
        self.per_minute = per_minute
        self.per_hour = per_hour
        self.spacing_seconds = spacing_seconds
        self._times: list[float] = []

    def _sleep_needed(self, now: float) -> float:
        # Drop anything outside the longest window we care about.
        self._times = [t for t in self._times if now - t < 3600.0]

        waits = [0.0]

        if self._times:
            waits.append(self.spacing_seconds - (now - self._times[-1]))

        recent_minute = [t for t in self._times if now - t < 60.0]
        if len(recent_minute) >= self.per_minute:
            # Wait until the oldest request in this minute falls out of it.
            waits.append(60.0 - (now - recent_minute[0]))

        if len(self._times) >= self.per_hour:
            waits.append(3600.0 - (now - self._times[0]))

        return max(waits)

    def acquire(self) -> None:
        """Block until another request is permitted, then record it."""
        while True:
            now = time.monotonic()
            wait = self._sleep_needed(now)
            if wait <= 0:
                self._times.append(now)
                return
            if wait > 5.0:
                logger.info("Rate limit: waiting %.0fs before next Space-Track request.", wait)
            time.sleep(wait)


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


def _query(
    session: requests.Session, query: str, limiter: Optional[_RateLimiter] = None
) -> list[dict]:
    """Run one Space-Track query and return its rows.

    Every caller should pass the SAME limiter instance, so the per-minute and
    per-hour budgets are shared across all resources in a run rather than being
    counted separately per resource.
    """
    if limiter is not None:
        limiter.acquire()

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
    history_days: int = 0,
) -> Any:
    """The full on-orbit GP catalogue, plus CDMs and optional historical backfill.

    Args:
        identity: Space-Track username. Auto-loaded from secrets.toml under
            [sources.space_track].
        password: Space-Track password. Auto-loaded the same way. Never logged.
        include_cdm: Attempt the CDM query. Leave True — the resource degrades
            to zero rows on a permission failure rather than erroring.
        history_days: Days of gp_history to backfill. DEFAULT 0 = OFF, because
            this is a one-off catch-up operation, not something a scheduled run
            should repeat every 8 hours. Each day costs one request, so 14 days
            is 14 requests spread over roughly a minute by the rate limiter.

            Why you would use it: the conjunction screening engine estimates
            covariance from the scatter across an object's recent element sets.
            With no history every event falls back to default sigmas. Backfilling
            two weeks makes that estimate real immediately instead of waiting two
            weeks for it to accumulate.
    """
    session = _login(identity, password)

    # One limiter shared by every resource, so the budget is counted across the
    # whole run rather than per resource.
    limiter = _RateLimiter()

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
        rows = _query(session, GP_QUERY, limiter)
        logger.info("Space-Track GP returned %d objects.", len(rows))
        yield from rows

    # Historical elsets, written to the SAME table as the current catalogue.
    #
    # That is deliberate rather than lazy: gp_history uses the same predicates
    # and returns the same shape as gp, and the screening engine's covariance
    # estimate wants one continuous per-object history to measure scatter
    # across. Splitting them would mean every consumer had to union two tables
    # and dedupe them. The merge key makes the overlap safe — a historical
    # elset that is also the current one collapses to a single row.
    @dlt.resource(
        name="space_track_gp",
        write_disposition="merge",
        primary_key=["NORAD_CAT_ID", "EPOCH"],
    )
    def gp_history() -> Iterator[dict]:
        if history_days <= 0:
            return

        logger.info(
            "Backfilling %d days of gp_history in %d-day chunks "
            "(~%d requests, rate limited to %d/min and %d/hr).",
            history_days,
            GP_HISTORY_CHUNK_DAYS,
            -(-history_days // GP_HISTORY_CHUNK_DAYS),
            MAX_REQUESTS_PER_MINUTE,
            MAX_REQUESTS_PER_HOUR,
        )

        today = datetime.now(timezone.utc).date()
        total = 0
        for offset in range(history_days, 0, -GP_HISTORY_CHUNK_DAYS):
            start = today - timedelta(days=offset)
            end = start + timedelta(days=GP_HISTORY_CHUNK_DAYS)
            query = GP_HISTORY_QUERY.format(
                start=start.isoformat(), end=end.isoformat()
            )
            try:
                rows = _query(session, query, limiter)
            except (requests.HTTPError, RuntimeError) as exc:
                # One bad window must not discard the windows already fetched.
                # Log it and continue: a backfill with a gap is still far more
                # useful than no backfill, and the gap is visible in the log.
                logger.warning(
                    "gp_history chunk %s..%s failed (%s). Continuing; this "
                    "window will be missing from the backfill.",
                    start,
                    end,
                    exc,
                )
                continue
            total += len(rows)
            logger.info("gp_history %s..%s -> %d elsets", start, end, len(rows))
            yield from rows

        logger.info("gp_history backfill complete: %d elsets.", total)

    @dlt.resource(
        name="space_track_cdm_public",
        write_disposition="merge",
        primary_key="CDM_ID",
    )
    def cdm_public() -> Iterator[dict]:
        try:
            rows = _query(session, CDM_QUERY, limiter)
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
    if history_days > 0:
        resources.append(gp_history)
    if include_cdm:
        resources.append(cdm_public)
    return resources


def load_space_track(
    identity: Optional[str] = None,
    password: Optional[str] = None,
    credentials: Optional[dict] = None,
    include_cdm: bool = True,
    history_days: int = 0,
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
        history_days: Days of gp_history to backfill. 0 (default) skips it —
            this is a one-off catch-up, not something a scheduled run repeats.
        dev_mode: Load into a fresh timestamped dataset instead of `raw`.
        destination_override: A dlt destination to use instead of Postgres, for
            smoke testing without warehouse credentials. Airflow never passes it.

    Returns:
        The dlt load info, stringified.
    """
    # Omitted args must stay *absent* rather than None, or an explicit None
    # would override dlt's secrets.toml resolution instead of deferring to it.
    source_kwargs: dict = {"include_cdm": include_cdm, "history_days": history_days}
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
