"""Launch Library 2 (The Space Devs) -> Postgres schema `raw`.

LL2 is the reference catalogue for launches, launch vehicles, agencies, pads,
astronauts and spacecraft. It is the dimensional backbone this platform has
otherwise been missing: CelesTrak and Space-Track say what is in orbit, LL2
says who put it there, on what, and from where.

Base: https://ll.thespacedevs.com/2.3.0/
Docs: https://ll.thespacedevs.com/2.3.0/docs/ and https://thespacedevs.com/llapi

THE RATE LIMIT IS THE ENTIRE DESIGN.
------------------------------------
`/api-throttle/` reports the truth for the calling identity. Anonymous, as of
2026-08-02, that is:

    {"your_request_limit": 15, "limit_frequency_secs": 3600, ...}

FIFTEEN REQUESTS PER HOUR. Not 15/minute. The `lldev` mirror is the same, so it
is no escape hatch. Page size caps at 100. A single complete pass over the
onboarded scope costs roughly 155 requests — about ten hours of budget — so
"fetch everything each run" is not an option that exists here.

Three consequences, and every awkward thing in this module follows from them:

  1. THE BUDGET IS DISCOVERED, NOT ASSUMED. Each run reads `/api-throttle/`
     first and sizes itself from the answer, including `current_use` — the
     ceiling is shared with anything else on this IP. If an API key is ever
     added to `LL2_API_KEY`, the reported limit rises and this module speeds up
     with no code change.

  2. RUNS ARE RESUMABLE. Every resource checkpoints its page offset in dlt
     state. When the budget runs out the run ends cleanly on a page boundary
     and the next run picks up exactly there. The first few daily runs are a
     rolling catch-up; after that the catalogue is complete and a run costs
     ~10 requests.

  3. DIMENSIONS REFRESH ON A ROTATION, NOT EVERY RUN. A table is re-pulled only
     once it is older than its `refresh_days`. Config lookups (`config/orbits`,
     `config/launch_statuses`) change perhaps yearly, so re-reading them daily
     would burn the entire budget on data that has not moved.

NO HISTORICAL BACKFILL, BY REQUEST AND BY NATURE. Unlike spaceflight_news,
nothing here is backfilled over a date range. These endpoints are dimensional:
they expose current state, not an append-only history, so there is no archive
to walk. The "catch-up" below is just completing the first full pass, which
15 requests/hour makes span a few days.

ONLY `launches` HAS A CURSOR. Verified live 2026-08-02: `agencies`, `pads`,
`programs`, `spacecraft`, `launcher_configurations` and every `config/*` table
have NO `last_updated` field at all, so there is nothing to read forward from
and a full re-pull is the only option. `launches` does have it, and supports
`last_updated__gte` (67 of 548 rows on the dev mirror), which is what keeps the
largest table (7,954 rows) cheap once it has been caught up.

PAGINATION IS BY `ordering=id`, ascending, with a page of overlap on resume.
Ids are assigned increasing, so new rows append at the END and never shift the
offsets of pages already read. `launches` is the exception — its id is a UUID,
which would insert randomly and shift everything after it — so launches orders
by `last_updated` ascending instead, where a revised row also moves to the end.
Neither ordering can move a row BACKWARD past an offset already consumed, which
is what makes a multi-run resume safe. The one-page overlap on resume covers
the remaining edge, where a row leaving the middle shifts its successors down.

RESPONSE MODE is left at the API default (`normal`) rather than `mode=list`.
`list` is a smaller payload but costs exactly the same number of requests —
and requests, not bytes, are the scarce resource here. It also drops the nested
mission/rocket/pad detail that makes `launches` worth having, and it is buggy:
`docking_events` 500s and `expeditions` 400s under `mode=list`.

Nested objects (`status`, `rocket`, `mission`, `pad`, ...) are flattened by dlt
into `mission__name`-style columns; nested arrays (`program`) become child
tables. That is expected, the same as nasa_donki.

Credentials: an API key is OPTIONAL and there is none today. If one is ever
obtained, pass it as `api_key` and it is sent as a `Token` header. The Postgres
credentials are passed in by the caller; in Airflow that is the DAG, reading
the `norion-analytics-pg` Connection.

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
from dlt.sources.helpers import requests

logger = logging.getLogger(__name__)

BASE_URL = "https://ll.thespacedevs.com/2.3.0"

# The Space Devs' development mirror. Same API, same 15/hour ceiling, but a
# small sampled dataset and a separate budget — which makes it the only way to
# exercise this module end to end without spending an hour of production quota.
# Never point a real load at it: it holds a few hundred launches, not 7,954.
DEV_BASE_URL = "https://lldev.thespacedevs.com/2.3.0"

# The Space Devs ask callers to be identifiable rather than anonymous.
USER_AGENT = "norion-warehouse/1.0 (+trevor.barnes91@gmail.com)"

# LL2's hard page cap. Asking for 1000 returns 100 — verified 2026-08-02.
PAGE_SIZE = 100

REQUEST_TIMEOUT_SECONDS = 120

# Fallback budget, used only if `/api-throttle/` cannot be read. Matches the
# documented anonymous allowance, so guessing wrong errs toward being polite.
FALLBACK_REQUEST_LIMIT = 15
FALLBACK_WINDOW_SECONDS = 3600

# Longest this run will wait for the rate-limit window to roll over. Slightly
# more than the 3600s window, so a run CAN sit through one rollover and keep
# going — which is what lets a single daily run spend ~60 requests instead of
# stopping dead after the first burst of 15.
#
# It is a ceiling, not a target: a run that has already exhausted `max_requests`
# stops immediately regardless. Sized against the LocalExecutor slot this holds.
MAX_SLEEP_SECONDS = 3700.0

# How far behind the run start the `launches` cursor is parked, covering clock
# skew and rows revised mid-walk. One cheap re-read per run; merge absorbs it.
CURSOR_LAG = timedelta(hours=12)

# Resources to load. `refresh_days` is how stale a table may get before it is
# re-pulled; it is the knob that keeps a 15/hour budget viable.
#
#   name                       path                          refresh_days
#
# Core dimensions — real editorial content that changes as the catalogue is
# curated, so weekly. Row counts measured live 2026-08-02.
LL2_DIMENSIONS: list[tuple[str, str, int]] = [
    ("ll2_agencies", "agencies", 7),                                    # 350
    ("ll2_astronauts", "astronauts", 7),                                # 858
    ("ll2_celestial_bodies", "celestial_bodies", 30),                   # 4
    ("ll2_launchers", "launchers", 7),                                  # 191
    ("ll2_launcher_configurations", "launcher_configurations", 7),      # 532
    ("ll2_launcher_configuration_families", "launcher_configuration_families", 30),  # 109
    ("ll2_locations", "locations", 7),
    ("ll2_pads", "pads", 7),                                            # 248
    ("ll2_programs", "programs", 7),                                    # 39
    ("ll2_spacecraft", "spacecraft", 7),                                # 607
    ("ll2_spacecraft_configurations", "spacecraft_configurations", 30),
    ("ll2_spacecraft_configuration_families", "spacecraft_configuration_families", 30),
    ("ll2_space_stations", "space_stations", 30),
]

# Config lookups — enumerations. These change on the order of once a year, so
# monthly is already generous. ~35 requests for the whole set, which is why
# they must NOT be on the daily path.
LL2_CONFIG_TABLES: list[str] = [
    "agency_types", "astronaut_roles", "astronaut_statuses", "astronaut_types",
    "celestial_body_types", "countries", "docking_locations", "event_types",
    "first_stage_types", "image_licenses", "image_variant_types", "infourl_types",
    "landing_locations", "landing_types", "languages", "launch_statuses",
    "launcher_statuses", "mission_types", "net_precisions", "notice_types",
    "orbits", "payload_types", "program_types", "road_closure_statuses",
    "spacecraft_configuration_types", "spacecraft_statuses", "space_station_statuses",
    "timeline_event_types", "vidurl_types",
]
CONFIG_REFRESH_DAYS = 30


class BudgetExhausted(Exception):
    """Raised when this run has spent its allotted requests.

    Not an error. It is the signal to stop cleanly on a page boundary and let
    the checkpoint in dlt state carry the work into the next run.
    """


class _Throttle:
    """Spend LL2 requests within the identity's real, discovered budget.

    Two mechanisms, deliberately separate:

      * A ROLLING WINDOW matching the server's own accounting, so a run that
        needs only a handful of requests issues them back to back and finishes
        in seconds. Pacing evenly instead would make a 10-request steady-state
        run take 40 minutes for no reason.
      * A PER-RUN CEILING (`max_requests`), which bounds how long one Airflow
        task may sit waiting for the window to roll over.

    Seeded from `current_use` so requests already spent by anything else on
    this IP — a notebook, a manual query — are not spent twice.
    """

    def __init__(
        self,
        max_requests: int,
        api_key: Optional[str] = None,
        base_url: str = BASE_URL,
    ) -> None:
        self.max_requests = max_requests
        self.base_url = base_url.rstrip("/")
        self.spent = 0
        self.limit = FALLBACK_REQUEST_LIMIT
        self.window = float(FALLBACK_WINDOW_SECONDS)
        self._headers = {"User-Agent": USER_AGENT}
        if api_key:
            self._headers["Authorization"] = f"Token {api_key}"
        # Monotonic timestamps of requests assumed to be counted against us.
        self._times: list[float] = []

    def probe(self) -> None:
        """Read `/api-throttle/` and size this run from the answer."""
        try:
            response = requests.get(
                f"{self.base_url}/api-throttle/",
                headers=self._headers,
                timeout=REQUEST_TIMEOUT_SECONDS,
            )
            response.raise_for_status()
            info = response.json()
        except Exception as exc:  # noqa: BLE001 - any failure falls back safely
            logger.warning(
                "Could not read LL2 /api-throttle/ (%s). Falling back to the "
                "documented anonymous budget of %d requests per %ds.",
                exc, FALLBACK_REQUEST_LIMIT, FALLBACK_WINDOW_SECONDS,
            )
            self._times = [time.monotonic()] * 1
            self.spent += 1
            return

        self.limit = int(info.get("your_request_limit") or FALLBACK_REQUEST_LIMIT)
        self.window = float(info.get("limit_frequency_secs") or FALLBACK_WINDOW_SECONDS)
        current_use = int(info.get("current_use") or 0)
        next_use = float(info.get("next_use_secs") or 0)

        # Seed conservatively: assume every already-spent request expires as
        # late as possible, i.e. `next_use` seconds from now. Erring this way
        # risks waiting slightly too long, never overrunning the server's count.
        now = time.monotonic()
        seeded_at = now - (self.window - next_use)
        self._times = [seeded_at] * min(current_use, self.limit)

        # The probe itself is a request.
        self._times.append(now)
        self.spent += 1

        logger.info(
            "LL2 budget: %d requests per %.0fs for ident %s; %d already used, "
            "this run will spend at most %d.",
            self.limit, self.window, info.get("ident", "?"), current_use, self.max_requests,
        )

    def acquire(self) -> None:
        """Block until another request is permitted. Raises BudgetExhausted."""
        if self.spent >= self.max_requests:
            raise BudgetExhausted(
                f"per-run ceiling of {self.max_requests} requests reached"
            )

        while True:
            now = time.monotonic()
            self._times = [t for t in self._times if now - t < self.window]
            if len(self._times) < self.limit:
                self._times.append(now)
                self.spent += 1
                return

            wait = self.window - (now - self._times[0]) + 1.0
            if wait > MAX_SLEEP_SECONDS:
                raise BudgetExhausted(
                    f"next LL2 request would need a {wait:.0f}s wait, beyond the "
                    f"{MAX_SLEEP_SECONDS:.0f}s this run will hold a worker for"
                )
            logger.info("LL2 rate limit: sleeping %.0fs for budget.", wait)
            time.sleep(wait)

    def get(self, path: str, params: dict) -> dict:
        """One rate-limited LL2 request. Returns the decoded envelope."""
        self.acquire()
        response = requests.get(
            f"{self.base_url}/{path}/",
            params=params,
            headers=self._headers,
            timeout=REQUEST_TIMEOUT_SECONDS,
        )

        if response.status_code == 429:
            # The budget calculation was wrong — something else spent from this
            # identity mid-run. Re-probe and give up on this run rather than
            # hammering an endpoint that is already refusing us.
            logger.warning(
                "LL2 returned 429 for %s despite the budget check. Ending this "
                "run; checkpoints will resume it next time.", path,
            )
            raise BudgetExhausted("upstream returned 429")

        response.raise_for_status()
        payload = response.json()
        if not isinstance(payload, dict) or "results" not in payload:
            raise RuntimeError(
                f"Unexpected LL2 response shape for {path}: {str(payload)[:200]}"
            )
        return payload


def _state_dir() -> str:
    """Where dlt keeps load history and working files.

    Must survive container restarts, so it goes on the bind mount rather than
    in the container's ephemeral home. Falls back to the repo copy when
    /opt/airflow is absent, which is what makes this runnable locally.
    """
    container_dir = Path("/opt/airflow/include/warehouse")
    base = container_dir if container_dir.is_dir() else Path(__file__).resolve().parents[1] / "warehouse"
    return str(base / ".dlt_pipelines")


def _is_fresh(last_refreshed: Optional[str], refresh_days: int) -> bool:
    """Has this table been fully pulled recently enough to skip?"""
    if not last_refreshed:
        return False
    try:
        when = datetime.fromisoformat(last_refreshed)
    except ValueError:
        return False
    return datetime.now(timezone.utc) - when < timedelta(days=refresh_days)


def _paged(
    throttle: _Throttle,
    path: str,
    state: dict,
    ordering: str,
    extra_params: Optional[dict] = None,
) -> Iterator[dict]:
    """Walk a collection from its checkpoint, yielding rows and saving progress.

    The checkpoint (`state["offset"]`) is what makes a 15-requests-per-hour
    budget workable: a run that cannot finish a table stops on a page boundary
    and the next run resumes there instead of starting over.

    Resumes one page EARLIER than the checkpoint. Ascending id/last_updated
    orderings only ever move a row toward the end, so a page already consumed
    cannot gain rows — but a row leaving the middle shifts its successors down
    by one, which without the overlap would skip a row at the boundary.
    Re-reading one page costs a single request and `merge` makes the duplicate
    rows free.
    """
    checkpoint = int(state.get("offset") or 0)
    offset = max(0, checkpoint - PAGE_SIZE) if checkpoint else 0

    while True:
        params = {"limit": PAGE_SIZE, "offset": offset, "ordering": ordering}
        if extra_params:
            params.update(extra_params)

        payload = throttle.get(path, params)
        rows = payload["results"]
        yield from rows

        offset += len(rows)
        # Checkpoint AFTER the rows are yielded, so a crash mid-page re-reads
        # that page rather than skipping it.
        state["offset"] = offset

        if not payload.get("next") or not rows:
            state["offset"] = 0
            logger.info("LL2 %s: complete at %d rows.", path, payload.get("count", offset))
            return


def _make_dimension(
    resource_name: str, path: str, refresh_days: int, throttle: _Throttle, force: bool
):
    """A full-pull LL2 collection, re-read only once it is stale enough."""

    @dlt.resource(name=resource_name, write_disposition="merge", primary_key="id")
    def dimension() -> Iterator[dict]:
        state = dlt.current.resource_state()
        in_progress = int(state.get("offset") or 0) > 0

        # A table mid-catch-up must continue regardless of freshness, or a
        # partially loaded table would be marked fresh and never finish.
        if not force and not in_progress and _is_fresh(state.get("last_refreshed"), refresh_days):
            logger.info(
                "LL2 %s: fresh (refreshed %s, every %dd) — skipping, 0 requests.",
                path, state.get("last_refreshed"), refresh_days,
            )
            return

        try:
            yield from _paged(throttle, path, state, ordering="id")
        except BudgetExhausted as exc:
            logger.info(
                "LL2 %s: paused at offset %s (%s). Resumes next run.",
                path, state.get("offset"), exc,
            )
            return

        # Only a completed pass counts as a refresh.
        state["last_refreshed"] = datetime.now(timezone.utc).isoformat()

    return dimension


def _make_launches(throttle: _Throttle, force: bool):
    """`launches` — the one LL2 collection with a usable incremental cursor."""

    @dlt.resource(name="ll2_launches", write_disposition="merge", primary_key="id")
    def launches() -> Iterator[dict]:
        state = dlt.current.resource_state()
        run_start = datetime.now(timezone.utc)
        cursor = None if force else state.get("cursor")
        in_progress = int(state.get("offset") or 0) > 0

        extra: dict = {}
        if cursor and not in_progress:
            extra["last_updated__gte"] = cursor
            logger.info("LL2 launches: incremental since %s.", cursor)
        else:
            logger.info(
                "LL2 launches: full pass (%s). ~80 requests, so expect this to "
                "span several runs at 15/hour.",
                "resuming" if in_progress else "no cursor yet",
            )

        try:
            # Ascending last_updated, NOT id: launch ids are UUIDs, which would
            # insert at random offsets and shift pages already read. A revised
            # launch moves to the END of this ordering, which a resume tolerates.
            yield from _paged(
                throttle, "launches", state, ordering="last_updated", extra_params=extra
            )
        except BudgetExhausted as exc:
            logger.info(
                "LL2 launches: paused at offset %s (%s). Resumes next run; the "
                "cursor is deliberately NOT advanced.",
                state.get("offset"), exc,
            )
            return

        # Clock-derived, and only after a complete pass. Everything that existed
        # at run_start has now been read, which is exactly what the cursor means.
        # Deriving it from max(last_updated) in the data would stall during a
        # catch-up, where old rows carry old timestamps.
        state["cursor"] = (run_start - CURSOR_LAG).isoformat()
        logger.info("LL2 launches: complete; cursor now %s.", state["cursor"])

    return launches


@dlt.source(name="launch_library")
def launch_library_source(
    api_key: Optional[str] = None,
    max_requests: int = 60,
    force_refresh: bool = False,
    base_url: str = BASE_URL,
) -> Any:
    """LL2 dimensions and the launch catalogue, within a discovered budget.

    Args:
        api_key: Optional Space Devs API key, sent as a `Token` header. There
            is none today; anonymous works and is simply slower. Adding one
            raises the limit `/api-throttle/` reports, and this module adapts
            with no other change.
        max_requests: Ceiling on requests for this run, INCLUDING the throttle
            probe. At the anonymous 15/hour, 60 is roughly a 3-hour task that
            bursts the first 15 immediately. Raise it for a deliberate
            catch-up; lower it to keep runs short.
        force_refresh: Ignore both the staleness rotation and the launches
            cursor and re-read everything. For repairing a bad load — it costs
            a full ~155-request pass, so it is not something to schedule.
        base_url: API root. Override only to smoke test against DEV_BASE_URL —
            never for a real load, which would fill `raw` with the mirror's
            sampled subset.
    """
    throttle = _Throttle(max_requests=max_requests, api_key=api_key, base_url=base_url)
    throttle.probe()

    # ORDER IS CHEAPEST-FIRST, and it matters because `extract.next_item_mode`
    # is fifo (see load_launch_library) — a budget-limited run drains this list
    # from the top and stops wherever the budget runs out.
    #
    # Config lookups first: ~35 requests buys all 29 of them, so ONE run leaves
    # the whole reference layer complete and queryable. Then the mid-size
    # dimensions. `launches` LAST, because it alone costs ~80 requests — put it
    # first and it would swallow the entire budget for two days while every
    # dimension table stayed empty, which is the opposite of useful.
    #
    # The consequence is deliberate: the dimensional warehouse is usable after
    # the first run, and the launch catalogue fills in over the following few.
    resources: list[Any] = [
        _make_dimension(
            f"ll2_config_{table}", f"config/{table}", CONFIG_REFRESH_DAYS, throttle, force_refresh
        )
        for table in LL2_CONFIG_TABLES
    ]
    resources += [
        _make_dimension(name, path, days, throttle, force_refresh)
        for name, path, days in LL2_DIMENSIONS
    ]
    resources.append(_make_launches(throttle, force_refresh))
    return resources


def load_launch_library(
    api_key: Optional[str] = None,
    credentials: Optional[dict] = None,
    max_requests: int = 60,
    force_refresh: bool = False,
    dev_mode: bool = False,
    destination_override: Optional[Any] = None,
    base_url: str = BASE_URL,
) -> str:
    """Load Launch Library 2 into the Postgres schema `raw`. Returns load info.

    Args:
        api_key: Optional Space Devs key. Omit for anonymous.
        credentials: Postgres connection as a dict of database/username/
            password/host/port. Omit to fall back to secrets.toml. The Airflow
            DAG builds this from the `norion-analytics-pg` Connection.
        max_requests: Per-run request ceiling. See the source docstring.
        force_refresh: Re-read everything, ignoring staleness and the cursor.
        dev_mode: Load into a fresh timestamped dataset instead of `raw`.
            Leave False for anything Airflow calls — dev datasets are invisible
            to dbt, AND a dev run gets a fresh state directory, so it would
            restart the catch-up from zero rather than resuming.
        destination_override: A dlt destination to use instead of Postgres, for
            smoke testing without warehouse credentials. Airflow never passes it.
        base_url: API root. Smoke testing only — see the source docstring.
    """
    if destination_override is not None:
        destination: Any = destination_override
    elif credentials:
        destination = dlt.destinations.postgres(credentials=credentials)
    else:
        destination = "postgres"

    # EXTRACT ONE RESOURCE AT A TIME. dlt's default `round_robin` takes a single
    # page from each resource in turn, which is actively wrong under a request
    # budget: with 43 resources, a 60-request run would spend ~1 page on each
    # and finish almost nothing. `launches` needs ~80 consecutive pages, and
    # round-robin would hand it one page per lap — it would take weeks.
    #
    # fifo drains each resource to completion before starting the next, so a
    # budget-limited run leaves whole tables loaded and one table checkpointed
    # mid-way, rather than forty-three tables all half-loaded. That also makes
    # the resource ORDER in launch_library_source meaningful.
    dlt.config["extract.next_item_mode"] = "fifo"

    pipeline = dlt.pipeline(
        pipeline_name="launch_library",
        destination=destination,
        dataset_name="raw",
        pipelines_dir=_state_dir(),
        dev_mode=dev_mode,
    )

    info = pipeline.run(
        launch_library_source(
            api_key=api_key,
            max_requests=max_requests,
            force_refresh=force_refresh,
            base_url=base_url,
        )
    )
    return str(info)


if __name__ == "__main__":
    # Smoke test: a handful of requests against the DEV MIRROR into local files.
    # Needs no warehouse credentials, leaves `raw` untouched, and spends the
    # mirror's budget rather than production's — which matters, because burning
    # production's 15/hour on a test locks the real pipeline out for an hour.
    logging.basicConfig(level=logging.INFO)
    print(  # noqa: T201
        load_launch_library(
            max_requests=6,
            dev_mode=True,
            base_url=DEV_BASE_URL,
            destination_override=dlt.destinations.filesystem(
                bucket_url="file:///tmp/ll2_smoke"
            ),
        )
    )
