"""SGP4 all-on-all conjunction screening -> Postgres schema `raw`.

Reads the latest element set per object from the staged catalogue, propagates
every object with SGP4, finds pairs whose separation drops below a threshold,
refines each to a true time of closest approach, and estimates a collision
probability from TLE-derived uncertainty.

WHAT THIS IS, AND WHAT IT IS NOT
--------------------------------
This produces conjunction SCREENING: candidate close approaches with a time of
closest approach and a miss distance. That part is sound.

It also produces an ESTIMATED probability of collision, and that number carries
a caveat that cannot be engineered away. Pc requires a covariance for both
objects. TLEs carry none — they are mean elements with no uncertainty attached,
and their position error is roughly 1-5 km radial and worse in-track, growing
with propagation time. A 500 m miss distance computed from SGP4 sits well
inside the propagator's own error bar.

The covariance used here is therefore SYNTHETIC: derived from the scatter of an
object's own recent element sets when propagated to a common epoch (the method
Kelso investigated for SOCRATES). It is a defensible empirical estimate of how
much the element sets disagree with each other. It is NOT the real covariance
from a tracking filter, and every column it feeds is named `estimated_*` so a
reader cannot mistake one for the other.

Deciding whether to manoeuvre a real spacecraft needs a CDM from the operator
or conjunction assessment provider, which carries a genuine covariance. This
pipeline is for triage, for learning the dynamics, and for noticing that
something deserves a closer look — not for the manoeuvre decision itself.

METHOD
------
1. Load the latest element set per object; initialise SGP4 from the OMM fields.
2. COARSE PASS: propagate every object over the screening window on a fixed
   grid, and at each step use a k-d tree to find all pairs within
   `coarse_radius_km`. Working in TEME throughout — both objects come out of
   SGP4 in the same frame, so relative geometry needs no conversion, and the
   frame conversion that would be required to report absolute positions is
   deliberately not done.
3. Reduce to one candidate episode per pair, at that pair's minimum coarse
   separation over the window.
4. FINE PASS: re-propagate each candidate pair on a sub-second grid around its
   coarse minimum to get a true TCA, miss distance and relative velocity.
5. Keep pairs whose refined miss distance is below `miss_threshold_km`.
6. Estimate covariance per object from element-set history and compute a 2D Pc
   in the encounter plane.

KNOWN LIMITATIONS, stated because they change how the output should be read:

  * The coarse grid can miss a fast head-on conjunction. Two objects closing at
    v_rel are up to v_rel * dt / 2 apart at the nearest sample, so the coarse
    radius must exceed that or the pair is never a candidate. The defaults
    (2 s, 25 km) cover closing speeds up to ~24 km/s, which spans the
    physically plausible range for Earth orbit, but tightening `coarse_step_s`
    is the lever if you distrust it.
  * Only ONE episode per pair per run is refined — the global coarse minimum.
    A pair with two distinct close approaches in the same window reports only
    the closer one.
  * ELEMENT-SET TWINS are suppressed, not reported. Two catalogue numbers can
    share one physical state — docked spacecraft such as NORAD 28358
    (INTELSAT 10-02) and 46113 (MEV-2), or duplicated entries. They produce a
    permanent 0 km / 0 km-s "conjunction" that would bury real events. The run
    row counts how many were suppressed.
  * Objects with fewer than `min_history_sets` element sets within
    `max_history_age_days` get a documented fallback covariance rather than an
    estimated one, flagged in the output. On a fresh install with no history
    every row says `fallback`; after a Space-Track gp_history backfill it was
    95.9% `tle_history`.
  * The covariance measures how much an object's RECENT element sets disagree,
    which is a proxy for state uncertainty and not the same thing. It is bounded
    to the last few days on purpose — over longer spans the scatter is dominated
    by SGP4 propagation error rather than by orbit-determination disagreement,
    which inflates the covariance and pushes Pc DOWN, understating risk.
  * `active` from CelesTrak is payloads only. Debris coverage depends on the
    Space-Track catalogue being loaded; without it this screens an incomplete
    universe and will silently under-report. The run row records how many
    objects were screened so this is visible rather than assumed.

IDEMPOTENCY AND BACKFILL
------------------------
`screening_run_id` is derived from the screening WINDOW START, not from
wall-clock time, so re-running an interval merges over its own previous output
instead of appending a second copy. That is what makes the pipeline safe to
re-run and makes Airflow catchup coherent.

Backfilling a past date requires `as_of` as well as `epoch_start`. The catalogue
query otherwise selects the LATEST element set per object, so a backfill without
`as_of` would propagate today's orbits from a past date — confident,
plausible-looking numbers that nobody could have predicted at the time, which is
worse than an error because nothing about the output looks wrong. `as_of` also
bounds the covariance history, without which the estimate would see how each
orbit actually resolved and report false confidence.

Backfill depth is limited by how much element-set history exists. CelesTrak
publishes only the current element set — there is no historical endpoint, so its
history accumulates forward only. Space-Track's `gp_history` class is the way to
obtain real depth; see the `history_days` argument on the space_track source.

This module deliberately imports nothing from Airflow, so it stays runnable and
testable outside the scheduler.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterator, Optional

import dlt
import numpy as np

logger = logging.getLogger(__name__)

# Screening window and resolution.
#
# The step and the radius are one decision, not two. A pair closing at v_rel is
# up to v_rel * step / 2 apart at the nearest sample, so to catch a miss of
# `m` the radius must exceed sqrt(m^2 + (v_rel * step / 2)^2). At 2 s and 25 km
# that holds for closing speeds up to ~24 km/s, which covers any physically
# plausible Earth-orbit encounter.
#
# Tuned empirically 2026-07-29 against 11,539 objects. The first attempt used
# 10 s / 100 km, which is the same capture guarantee on paper but accumulates
# candidate pairs so fast that it blew the safety cap after 600 of 8,640 steps
# and silently screened only the first 100 minutes of the day. A tighter radius
# is what makes a full-window screen affordable: candidate volume goes as the
# cube of the radius, so halving it is an 8x reduction.
DEFAULT_WINDOW_HOURS = 24
DEFAULT_COARSE_STEP_S = 2.0
DEFAULT_COARSE_RADIUS_KM = 25.0
DEFAULT_MISS_THRESHOLD_KM = 5.0

# Fine pass: +/- one coarse step around the candidate minimum.
DEFAULT_FINE_STEP_S = 0.25

# Covariance estimation.
DEFAULT_MIN_HISTORY_SETS = 3
DEFAULT_MAX_HISTORY_SETS = 12

# Only element sets from the last few days feed the scatter estimate.
#
# This bound is not optional, and a count limit alone does NOT substitute for
# it. The estimate propagates each historical element set to one common epoch
# and measures how far apart they land. An OLD element set lands far away
# largely because SGP4 error grows with propagation time — in-track error
# especially — so including it measures propagation age rather than
# orbit-determination disagreement.
#
# Measured 2026-07-29 after the gp_history backfill gave ~27 sets per object
# over 14 days: with only the count limit, 12 sets reached back 5-6 days and
# 12.8% of events came out with a combined in-track sigma above 1000 km, with
# a maximum of 9082 km. Those are not credible uncertainties, and because a
# larger covariance spreads the probability out, they push Pc DOWN — silently
# understating risk, which is the dangerous direction to be wrong in.
#
# The window trades estimate QUALITY against estimate COVERAGE. Measured against
# the live catalogue 2026-07-29 (32k objects, 14 days of gp_history):
#
#   window   objects with 3+ sets   avg sets   events with implausible sigma
#    2 d            32.1%              2.1
#    3 d            80.6%              3.9              2.0%   <- chosen
#    5 d            91.5%              6.4
#    7 d            94.0%             10.5
#   14 d            97.0%             25.2             12.8%
#
# Three days is chosen because an honest `fallback` flag is better than a
# confident number built on inflated covariance. Widening to 5 d buys ~11 more
# points of coverage at the cost of longer propagation intervals; that is the
# knob to turn if fallback coverage matters more than sigma quality, and it is a
# judgement call rather than a fact.
DEFAULT_MAX_HISTORY_AGE_DAYS = 3.0

# Fallback 1-sigma position uncertainty when an object has too little element
# set history to estimate one. These are the widely quoted TLE error magnitudes
# — order 1 km radial and cross-track, several km in-track — and are used only
# so such an object still gets a Pc rather than being dropped silently. Rows
# using them are flagged with covariance_source = 'fallback'.
FALLBACK_SIGMA_RADIAL_KM = 1.0
FALLBACK_SIGMA_INTRACK_KM = 5.0
FALLBACK_SIGMA_CROSSTRACK_KM = 1.0

# Combined hard-body radius, metres->km. Both objects' physical size matters for
# Pc and the catalogue does not publish dimensions, so a single conservative
# default stands in for both. 10 m combined is a common screening convention.
DEFAULT_HARD_BODY_RADIUS_KM = 0.010

# Safety valve. An unexpectedly dense candidate set would otherwise make the
# fine pass run for hours inside a DAG task. If this trips, the run row records
# it and the log says so — a truncated screen must never look like a complete
# one.
MAX_CANDIDATE_PAIRS = 2_000_000


def _state_dir() -> str:
    """Where dlt keeps load history and working files.

    Must survive container restarts, so it goes on the bind mount rather than
    in the container's ephemeral home. Falls back to the repo copy when
    /opt/airflow is absent, which is what makes this runnable locally.
    """
    container_dir = Path("/opt/airflow/include/warehouse")
    base = container_dir if container_dir.is_dir() else Path(__file__).resolve().parents[1] / "warehouse"
    return str(base / ".dlt_pipelines")


# --------------------------------------------------------------------------
# Catalogue loading
# --------------------------------------------------------------------------

# Latest element set per object, unioned across both catalogue sources.
#
# Space-Track is preferred over CelesTrak for the same object because it is the
# authoritative catalogue and covers debris; CelesTrak fills in whatever
# Space-Track has not been loaded for. source_rank encodes that preference and
# DISTINCT ON collapses to one row per object.
#
# The space_track branch is wrapped so this still runs before that source is
# onboarded — see _load_catalogue.
CATALOGUE_SQL = """
select distinct on (norad_cat_id)
    norad_cat_id,
    object_name,
    epoch,
    mean_motion_rev_per_day,
    eccentricity,
    inclination_deg,
    raan_deg,
    arg_of_pericenter_deg,
    mean_anomaly_deg,
    bstar,
    mean_motion_dot,
    mean_motion_ddot,
    ephemeris_type,
    classification_type,
    element_set_no,
    rev_at_epoch,
    catalogue_source
from (
    {branches}
) unioned
order by norad_cat_id, source_rank, epoch desc
"""

_CELESTRAK_BRANCH = """
    select
        norad_cat_id, object_name, epoch, mean_motion_rev_per_day, eccentricity,
        inclination_deg, raan_deg, arg_of_pericenter_deg, mean_anomaly_deg,
        bstar, mean_motion_dot, mean_motion_ddot, ephemeris_type,
        classification_type, element_set_no, rev_at_epoch,
        'celestrak' as catalogue_source, 2 as source_rank
    from analytics.stg_celestrak__gp
    {as_of_filter}
"""

_SPACE_TRACK_BRANCH = """
    select
        norad_cat_id, object_name, epoch, mean_motion_rev_per_day, eccentricity,
        inclination_deg, raan_deg, arg_of_pericenter_deg, mean_anomaly_deg,
        bstar, mean_motion_dot, mean_motion_ddot, ephemeris_type,
        classification_type, element_set_no, rev_at_epoch,
        'space_track' as catalogue_source, 1 as source_rank
    from analytics.stg_space_track__gp
    {as_of_filter}
"""

# Applied to every catalogue branch when screening a past date.
#
# Without it a backfill selects TODAY'S element set and propagates it from a
# past epoch, which produces confident, plausible-looking numbers that are not
# what anyone could have predicted at the time. That is worse than an error,
# because nothing about the output looks wrong.
_AS_OF_FILTER = "where epoch <= %(as_of)s"

# Recent element sets per object, for the covariance estimate. Ordered newest
# first so a LIMIT per object takes the most recent ones.
# Recent element sets per object, for the covariance estimate.
#
# The as_of bound applies here too, and for a subtler reason than the catalogue
# query: element sets published AFTER the screening moment would let the
# covariance estimate see how the orbit actually resolved. That is look-ahead
# bias, and it would make a backfilled screen look more certain than the real
# one ever could have been.
HISTORY_SQL = """
select
    norad_cat_id, object_name, epoch, mean_motion_rev_per_day, eccentricity,
    inclination_deg, raan_deg, arg_of_pericenter_deg, mean_anomaly_deg, bstar,
    mean_motion_dot, mean_motion_ddot, ephemeris_type, classification_type,
    element_set_no, rev_at_epoch
from (
    select *, row_number() over (partition by norad_cat_id order by epoch desc) as rn
    from (
        -- Deduplicate across sources before ranking. The same (object, epoch)
        -- can appear in both catalogues, and a duplicated element set would be
        -- counted as agreement — understating the scatter and reporting more
        -- confidence than the data supports.
        select distinct on (norad_cat_id, epoch) *
        from (
            {branches}
        ) unioned
        order by norad_cat_id, epoch, source_rank
    ) deduped
    where norad_cat_id = any(%(ids)s)
      and (%(as_of)s is null or epoch <= %(as_of)s)
      -- Age floor, not just a count limit. See DEFAULT_MAX_HISTORY_AGE_DAYS.
      and epoch >= %(history_floor)s
) ranked
where rn <= %(max_sets)s
order by norad_cat_id, epoch desc
"""

# History branches. Same sources as the catalogue, but selected across ALL
# epochs rather than collapsed to the latest one.
#
# Space-Track is what makes this work at all: its gp_history backfill gives ~32
# element sets per object, while CelesTrak publishes only the current set and
# accumulates roughly one per day going forward. Reading CelesTrak alone — which
# this did originally — leaves every object below min_history_sets, so every Pc
# silently falls back to default sigmas no matter how much history exists
# elsewhere.
_HISTORY_COLUMNS = """
        norad_cat_id, object_name, epoch, mean_motion_rev_per_day, eccentricity,
        inclination_deg, raan_deg, arg_of_pericenter_deg, mean_anomaly_deg,
        bstar, mean_motion_dot, mean_motion_ddot, ephemeris_type,
        classification_type, element_set_no, rev_at_epoch
"""

_HISTORY_CELESTRAK_BRANCH = f"""
    select {_HISTORY_COLUMNS}, 2 as source_rank
    from analytics.stg_celestrak__gp
"""

_HISTORY_SPACE_TRACK_BRANCH = f"""
    select {_HISTORY_COLUMNS}, 1 as source_rank
    from analytics.stg_space_track__gp
"""


def _table_exists(cur: Any, schema: str, name: str) -> bool:
    cur.execute(
        "select 1 from information_schema.tables where table_schema=%s and table_name=%s",
        (schema, name),
    )
    return cur.fetchone() is not None


def _load_catalogue(cur: Any, as_of: Optional[datetime] = None) -> list[dict]:
    """Element set current as of `as_of`, per object, preferring Space-Track.

    With as_of=None this is the latest element set per object — the live case.
    With as_of set it is the element set that was current at that moment, which
    is what makes a backfilled screen mean anything.
    """
    branches = [_CELESTRAK_BRANCH]
    if _table_exists(cur, "analytics", "stg_space_track__gp"):
        branches.insert(0, _SPACE_TRACK_BRANCH)
        logger.info("Space-Track catalogue present; it takes precedence over CelesTrak.")
    else:
        logger.warning(
            "stg_space_track__gp not found. Screening CelesTrak only, which is "
            "ACTIVE PAYLOADS ONLY — debris is absent and this screen will "
            "under-report. Onboard the space_track source for full coverage."
        )

    as_of_filter = _AS_OF_FILTER if as_of is not None else ""
    sql = CATALOGUE_SQL.format(
        branches=" union all ".join(b.format(as_of_filter=as_of_filter) for b in branches)
    )
    cur.execute(sql, {"as_of": as_of} if as_of is not None else None)
    columns = [d[0] for d in cur.description]
    rows = [dict(zip(columns, row)) for row in cur.fetchall()]

    if as_of is not None:
        logger.info(
            "Backfill mode: selected the element set current as of %s (%d objects).",
            as_of.isoformat(),
            len(rows),
        )
    return rows


def _init_satrec(record: dict) -> Any:
    """Build an SGP4 Satrec from one staged element set row.

    Uses sgp4.omm rather than the TLE text path: the warehouse stores OMM
    fields as typed columns, and round-tripping them through 69-character TLE
    strings would lose precision and reintroduce the Alpha-5 catalogue-number
    problem for objects above 99,999.
    """
    from sgp4 import omm
    from sgp4.api import Satrec

    epoch: datetime = record["epoch"]
    fields = {
        "OBJECT_ID": str(record.get("object_name") or record["norad_cat_id"]),
        "EPOCH": epoch.strftime("%Y-%m-%dT%H:%M:%S.%f"),
        "MEAN_MOTION": float(record["mean_motion_rev_per_day"]),
        "ECCENTRICITY": float(record["eccentricity"]),
        "INCLINATION": float(record["inclination_deg"]),
        "RA_OF_ASC_NODE": float(record["raan_deg"]),
        "ARG_OF_PERICENTER": float(record["arg_of_pericenter_deg"]),
        "MEAN_ANOMALY": float(record["mean_anomaly_deg"]),
        "BSTAR": float(record["bstar"] or 0.0),
        "MEAN_MOTION_DOT": float(record["mean_motion_dot"] or 0.0),
        "MEAN_MOTION_DDOT": float(record["mean_motion_ddot"] or 0.0),
        "EPHEMERIS_TYPE": int(record["ephemeris_type"] or 0),
        "CLASSIFICATION_TYPE": record["classification_type"] or "U",
        "NORAD_CAT_ID": str(record["norad_cat_id"]),
        "ELEMENT_SET_NO": int(record["element_set_no"] or 999),
        "REV_AT_EPOCH": int(record["rev_at_epoch"] or 0),
    }
    sat = Satrec()
    omm.initialize(sat, fields)
    return sat


# --------------------------------------------------------------------------
# Coarse pass
# --------------------------------------------------------------------------


def _coarse_pass(
    sat_array: Any,
    jd_base: float,
    fr_offsets: np.ndarray,
    coarse_radius_km: float,
    chunk_steps: int,
) -> dict[tuple[int, int], tuple[float, int]]:
    """Find candidate pairs and each pair's minimum coarse separation.

    Returns {(i, j): (min_separation_km, step_index)}.

    Propagation is chunked over TIME rather than done in one call: the position
    array is nsat x nsteps x 3 float64, which at full catalogue scale over a
    day is gigabytes. Chunking keeps peak memory to tens of megabytes and costs
    nothing in speed.
    """
    from scipy.spatial import cKDTree

    best: dict[tuple[int, int], tuple[float, int]] = {}
    n_steps = len(fr_offsets)
    truncated = False

    for start in range(0, n_steps, chunk_steps):
        stop = min(start + chunk_steps, n_steps)
        fr_chunk = fr_offsets[start:stop]
        jd_chunk = np.full(len(fr_chunk), jd_base)

        errors, positions, _ = sat_array.sgp4(jd_chunk, fr_chunk)

        for local_step in range(positions.shape[1]):
            step = start + local_step
            pos = positions[:, local_step, :]
            ok = errors[:, local_step] == 0
            # A decayed or numerically diverged object returns a nonzero error
            # code and garbage positions; including it would manufacture
            # spurious conjunctions.
            idx = np.flatnonzero(ok)
            if idx.size < 2:
                continue

            tree = cKDTree(pos[idx])
            for a, b in tree.query_pairs(coarse_radius_km):
                i, j = int(idx[a]), int(idx[b])
                if i > j:
                    i, j = j, i
                sep = float(np.linalg.norm(pos[i] - pos[j]))
                prev = best.get((i, j))
                if prev is None or sep < prev[0]:
                    best[(i, j)] = (sep, step)

            if len(best) > MAX_CANDIDATE_PAIRS:
                truncated = True
                break
        if truncated:
            break

    if truncated:
        logger.warning(
            "Coarse pass hit MAX_CANDIDATE_PAIRS (%d) and STOPPED EARLY at step "
            "%d of %d. This screen is INCOMPLETE — later times in the window "
            "were never examined. Raise the cap or narrow the window.",
            MAX_CANDIDATE_PAIRS,
            start,
            n_steps,
        )

    return best


# --------------------------------------------------------------------------
# Fine pass
# --------------------------------------------------------------------------


def _refine_pair(
    sat_i: Any,
    sat_j: Any,
    jd_base: float,
    fr_centre: float,
    half_width_s: float,
    fine_step_s: float,
) -> Optional[tuple[float, float, float, np.ndarray, np.ndarray]]:
    """Refine one pair to its true closest approach.

    Returns (miss_km, tca_fr_offset, rel_speed_kms, rel_pos, rel_vel), or None
    if either object failed to propagate.
    """
    from sgp4.api import SatrecArray

    n = max(3, int(2 * half_width_s / fine_step_s) + 1)
    offsets = np.linspace(-half_width_s, half_width_s, n) / 86400.0
    fr = fr_centre + offsets
    jd = np.full(n, jd_base)

    arr = SatrecArray([sat_i, sat_j])
    errors, positions, velocities = arr.sgp4(jd, fr)

    good = (errors[0] == 0) & (errors[1] == 0)
    if not good.any():
        return None

    rel = positions[0] - positions[1]
    dist = np.linalg.norm(rel, axis=1)
    dist = np.where(good, dist, np.inf)

    k = int(np.argmin(dist))
    if not np.isfinite(dist[k]):
        return None

    rel_vel = velocities[0][k] - velocities[1][k]
    return (
        float(dist[k]),
        float(fr[k]),
        float(np.linalg.norm(rel_vel)),
        rel[k],
        rel_vel,
    )


# --------------------------------------------------------------------------
# Pseudo-covariance and Pc
# --------------------------------------------------------------------------


def _ric_basis(position: np.ndarray, velocity: np.ndarray) -> np.ndarray:
    """Radial / in-track / cross-track unit vectors as rows.

    Falls back to the identity when the inputs are degenerate — a zero-length
    position or a position-velocity pair that is exactly parallel. That happens
    for co-located objects (see ELEMENT-SET TWINS in the module docstring), and
    without the guard numpy divides by zero and quietly seeds NaN through the
    whole Pc calculation.
    """
    pos_norm = np.linalg.norm(position)
    if pos_norm == 0 or not np.isfinite(pos_norm):
        return np.eye(3)

    radial = position / pos_norm
    cross = np.cross(position, velocity)
    cross_norm = np.linalg.norm(cross)
    if cross_norm == 0 or not np.isfinite(cross_norm):
        return np.eye(3)

    crosstrack = cross / cross_norm
    intrack = np.cross(crosstrack, radial)
    return np.vstack([radial, intrack, crosstrack])


def _element_fingerprint(record: dict) -> tuple:
    """Identify objects sharing one physical state.

    Two catalogue numbers can carry a byte-identical element set. The case that
    prompted this: NORAD 28358 (INTELSAT 10-02) and 46113 (MEV-2), which have
    been physically DOCKED since 2021 and are tracked as one object under two
    numbers. Their element sets match exactly, so the screen reported a 0.000 km
    conjunction at 0.00 km/s between them — every run, forever.

    Docked spacecraft, deployment pairs still attached, and duplicated catalogue
    entries all produce this. They are not conjunctions, and reporting them
    buries real events under permanent phantoms.

    Matching on the full element set rather than on position alone is
    deliberate: two objects genuinely flying in close formation have similar but
    not identical elements, and those ARE worth reporting.
    """
    return (
        record["epoch"],
        record["mean_motion_rev_per_day"],
        record["eccentricity"],
        record["inclination_deg"],
        record["raan_deg"],
        record["arg_of_pericenter_deg"],
        record["mean_anomaly_deg"],
    )


def _estimate_sigmas(
    history: list[dict], jd_base: float, fr_target: float
) -> Optional[tuple[float, float, float]]:
    """1-sigma RIC position uncertainty from element-set scatter.

    Propagates each of an object's recent element sets to the SAME epoch and
    measures how much they disagree. That disagreement is the only empirical
    uncertainty signal a TLE-based pipeline has: it captures how much the
    orbit determination moved between updates, which correlates with — but is
    not the same as — the true state uncertainty.

    Returns None when there is too little history to say anything.
    """
    from sgp4.api import SatrecArray

    if len(history) < DEFAULT_MIN_HISTORY_SETS:
        return None

    sats = []
    for record in history:
        try:
            sats.append(_init_satrec(record))
        except Exception:  # noqa: BLE001 - a bad historical set must not kill the run
            continue
    if len(sats) < DEFAULT_MIN_HISTORY_SETS:
        return None

    arr = SatrecArray(sats)
    errors, positions, velocities = arr.sgp4(np.array([jd_base]), np.array([fr_target]))
    ok = errors[:, 0] == 0
    if ok.sum() < DEFAULT_MIN_HISTORY_SETS:
        return None

    pos = positions[ok, 0, :]
    vel = velocities[ok, 0, :]

    # Scatter about the mean, expressed in the RIC frame of the mean state.
    mean_pos = pos.mean(axis=0)
    mean_vel = vel.mean(axis=0)
    basis = _ric_basis(mean_pos, mean_vel)
    residuals = (pos - mean_pos) @ basis.T

    # ddof=1: this is a sample standard deviation over a handful of element
    # sets, not a population.
    sigmas = residuals.std(axis=0, ddof=1)
    return float(sigmas[0]), float(sigmas[1]), float(sigmas[2])


def _pc_foster_2d(
    rel_pos: np.ndarray,
    rel_vel: np.ndarray,
    sigma_i: tuple[float, float, float],
    sigma_j: tuple[float, float, float],
    basis_i: np.ndarray,
    basis_j: np.ndarray,
    hard_body_radius_km: float,
) -> float:
    """2D probability of collision in the encounter plane.

    The standard short-encounter formulation: for a fast, near-linear flyby the
    3D integral collapses onto the plane perpendicular to the relative velocity,
    where the combined position uncertainty is a 2D Gaussian and the collision
    cross-section is a circle of the combined hard-body radius.

    Integrated numerically on a polar grid rather than via a series expansion —
    the grid is tiny, and it stays correct in the near-circular and
    highly-elongated covariance cases alike, where truncated series can misbehave.
    """
    v_rel = np.linalg.norm(rel_vel)
    if v_rel <= 0:
        return 0.0

    # Combined covariance in inertial axes: each object's diagonal RIC
    # covariance rotated into TEME and summed. Summing is valid because the two
    # objects' errors are treated as independent, which is the standard
    # assumption when they come from separate orbit determinations.
    cov_i = basis_i.T @ np.diag(np.array(sigma_i) ** 2) @ basis_i
    cov_j = basis_j.T @ np.diag(np.array(sigma_j) ** 2) @ basis_j
    cov = cov_i + cov_j

    # Encounter-plane basis: perpendicular to relative velocity.
    v_hat = rel_vel / v_rel
    seed = np.array([1.0, 0.0, 0.0])
    if abs(np.dot(seed, v_hat)) > 0.9:
        seed = np.array([0.0, 1.0, 0.0])
    x_hat = np.cross(v_hat, seed)
    x_hat /= np.linalg.norm(x_hat)
    y_hat = np.cross(v_hat, x_hat)

    plane = np.vstack([x_hat, y_hat])
    cov_2d = plane @ cov @ plane.T
    miss_2d = plane @ rel_pos

    try:
        inv = np.linalg.inv(cov_2d)
    except np.linalg.LinAlgError:
        return 0.0
    det = np.linalg.det(cov_2d)
    if det <= 0:
        return 0.0

    # Polar grid over the hard-body disc, centred on the projected miss vector.
    n_r, n_theta = 24, 48
    r_edges = np.linspace(0.0, hard_body_radius_km, n_r + 1)
    r_mid = 0.5 * (r_edges[:-1] + r_edges[1:])
    dr = r_edges[1] - r_edges[0]
    theta = np.linspace(0.0, 2 * np.pi, n_theta, endpoint=False)
    dtheta = 2 * np.pi / n_theta

    rr, tt = np.meshgrid(r_mid, theta, indexing="ij")
    xs = rr * np.cos(tt) - miss_2d[0]
    ys = rr * np.sin(tt) - miss_2d[1]

    quad = inv[0, 0] * xs**2 + 2 * inv[0, 1] * xs * ys + inv[1, 1] * ys**2
    density = np.exp(-0.5 * quad) / (2 * np.pi * np.sqrt(det))

    return float(np.sum(density * rr * dr * dtheta))


# --------------------------------------------------------------------------
# Orchestration
# --------------------------------------------------------------------------


def screen_catalogue(
    dsn: dict,
    window_hours: float = DEFAULT_WINDOW_HOURS,
    coarse_step_s: float = DEFAULT_COARSE_STEP_S,
    coarse_radius_km: float = DEFAULT_COARSE_RADIUS_KM,
    miss_threshold_km: float = DEFAULT_MISS_THRESHOLD_KM,
    fine_step_s: float = DEFAULT_FINE_STEP_S,
    hard_body_radius_km: float = DEFAULT_HARD_BODY_RADIUS_KM,
    max_objects: Optional[int] = None,
    epoch_start: Optional[datetime] = None,
    as_of: Optional[datetime] = None,
) -> tuple[dict, list[dict]]:
    """Run one full screen. Returns (run_summary, events).

    Args:
        epoch_start: When the screening window begins. Defaults to now.
        as_of: Treat this as "the present" when choosing element sets — pick the
            set current at this moment rather than the latest one, and ignore
            history published after it. Set this for any backfill. Passing
            epoch_start into the past WITHOUT as_of propagates today's elements
            from a past date, which is physically meaningless; the two are
            expected to move together and the loader defaults as_of to
            epoch_start for exactly that reason.
    """
    import psycopg2
    from sgp4.api import SatrecArray, jday

    started = datetime.now(timezone.utc)
    window_start = epoch_start or started

    # The connection stays open past this point: the covariance step needs a
    # second query, and which objects it asks about is not known until the fine
    # pass has decided which conjunctions survive.
    conn = psycopg2.connect(**dsn)
    cur = conn.cursor()
    catalogue = _load_catalogue(cur, as_of=as_of)

    if max_objects is not None:
        catalogue = catalogue[:max_objects]

    sats: list[Any] = []
    meta: list[dict] = []
    init_failures = 0
    for record in catalogue:
        try:
            sats.append(_init_satrec(record))
            meta.append(record)
        except Exception:  # noqa: BLE001
            init_failures += 1

    logger.info(
        "Screening %d objects (%d failed to initialise) over %.1f h at %.1f s resolution.",
        len(sats),
        init_failures,
        window_hours,
        coarse_step_s,
    )
    if len(sats) < 2:
        conn.close()
        raise RuntimeError("Fewer than two objects available to screen.")

    jd_base, fr_base = jday(
        window_start.year,
        window_start.month,
        window_start.day,
        window_start.hour,
        window_start.minute,
        window_start.second + window_start.microsecond / 1e6,
    )
    n_steps = int(window_hours * 3600 / coarse_step_s)
    fr_offsets = fr_base + np.arange(n_steps) * (coarse_step_s / 86400.0)

    sat_array = SatrecArray(sats)

    # ~600 steps keeps the position array around 150 MB at full catalogue scale.
    chunk_steps = max(1, min(600, n_steps))
    candidates = _coarse_pass(sat_array, jd_base, fr_offsets, coarse_radius_km, chunk_steps)
    logger.info("Coarse pass found %d candidate pairs.", len(candidates))

    # Fine pass. Only pairs whose coarse minimum is plausibly refinable are
    # worth the cost: a pair 90 km apart at its coarse minimum cannot refine to
    # under 5 km within half a coarse step.
    max_closing_km = coarse_radius_km
    refined: list[dict] = []
    fingerprints = [_element_fingerprint(m) for m in meta]
    element_set_twins = 0

    for (i, j), (coarse_sep, step) in candidates.items():
        if coarse_sep > max_closing_km:
            continue
        # Same physical object under two catalogue numbers — docked spacecraft
        # or a duplicated entry. Not a conjunction. See _element_fingerprint.
        if fingerprints[i] == fingerprints[j]:
            element_set_twins += 1
            continue
        result = _refine_pair(
            sats[i], sats[j], jd_base, float(fr_offsets[step]), coarse_step_s, fine_step_s
        )
        if result is None:
            continue
        miss_km, tca_fr, rel_speed, rel_pos, rel_vel = result
        if miss_km > miss_threshold_km:
            continue
        refined.append(
            {
                "i": i,
                "j": j,
                "miss_km": miss_km,
                "tca_fr": tca_fr,
                "rel_speed_kms": rel_speed,
                "rel_pos": rel_pos,
                "rel_vel": rel_vel,
                "coarse_separation_km": coarse_sep,
            }
        )

    logger.info(
        "Fine pass kept %d conjunctions under %.1f km (suppressed %d element-set "
        "twins — co-located or docked objects sharing one element set).",
        len(refined),
        miss_threshold_km,
        element_set_twins,
    )

    # Covariance estimation, only for objects that actually appear in a kept
    # conjunction — estimating for all 11k+ would dominate the runtime.
    involved = sorted({r["i"] for r in refined} | {r["j"] for r in refined})
    sigmas: dict[int, tuple[float, float, float]] = {}
    sources: dict[int, str] = {}

    if involved:
        ids = [int(meta[k]["norad_cat_id"]) for k in involved]
        # Same source availability rule as the catalogue query: use Space-Track
        # when it is present, and fall back to CelesTrak alone when it is not.
        history_branches = [_HISTORY_CELESTRAK_BRANCH]
        if _table_exists(cur, "analytics", "stg_space_track__gp"):
            history_branches.insert(0, _HISTORY_SPACE_TRACK_BRANCH)

        cur.execute(
            HISTORY_SQL.format(branches=" union all ".join(history_branches)),
            {
                "ids": ids,
                "max_sets": DEFAULT_MAX_HISTORY_SETS,
                "as_of": as_of,
                # Anchored to the screening moment, not to wall-clock now, so a
                # backfill uses the history that was recent AT THE TIME.
                "history_floor": (as_of or window_start)
                - timedelta(days=DEFAULT_MAX_HISTORY_AGE_DAYS),
            },
        )
        columns = [d[0] for d in cur.description]
        by_object: dict[int, list[dict]] = {}
        for row in cur.fetchall():
            record = dict(zip(columns, row))
            by_object.setdefault(int(record["norad_cat_id"]), []).append(record)

        for k in involved:
            norad = int(meta[k]["norad_cat_id"])
            estimate = _estimate_sigmas(by_object.get(norad, []), jd_base, fr_base)
            if estimate is None:
                sigmas[k] = (
                    FALLBACK_SIGMA_RADIAL_KM,
                    FALLBACK_SIGMA_INTRACK_KM,
                    FALLBACK_SIGMA_CROSSTRACK_KM,
                )
                sources[k] = "fallback"
            else:
                sigmas[k] = estimate
                sources[k] = "tle_history"

    conn.close()

    # Derived from the screening WINDOW, not from wall-clock time.
    #
    # This is what makes the pipeline idempotent. Keying on datetime.now()
    # minted a fresh id on every execution, so re-running the same interval
    # appended a second full set of rows instead of correcting the first.
    # Keying on window_start means a re-run merges over its own previous
    # output, and Airflow catchup becomes coherent.
    run_id = window_start.strftime("%Y%m%dT%H%M%SZ")
    events: list[dict] = []

    for r in refined:
        i, j = r["i"], r["j"]
        tca = window_start + timedelta(days=float(r["tca_fr"] - fr_base))

        basis = _ric_basis(r["rel_pos"], r["rel_vel"])
        estimated_pc = _pc_foster_2d(
            r["rel_pos"],
            r["rel_vel"],
            sigmas[i],
            sigmas[j],
            basis,
            basis,
            hard_body_radius_km,
        )

        covariance_source = (
            "tle_history"
            if sources.get(i) == "tle_history" and sources.get(j) == "tle_history"
            else "fallback"
        )

        events.append(
            {
                "screening_run_id": run_id,
                "primary_norad_cat_id": int(meta[i]["norad_cat_id"]),
                "primary_object_name": meta[i]["object_name"],
                "primary_catalogue_source": meta[i]["catalogue_source"],
                "secondary_norad_cat_id": int(meta[j]["norad_cat_id"]),
                "secondary_object_name": meta[j]["object_name"],
                "secondary_catalogue_source": meta[j]["catalogue_source"],
                "tca": tca.isoformat(),
                "miss_distance_km": r["miss_km"],
                "relative_speed_km_s": r["rel_speed_kms"],
                "coarse_separation_km": r["coarse_separation_km"],
                # Every uncertainty-derived field is prefixed `estimated_`
                # because none of it comes from a real covariance. See the
                # module docstring.
                "estimated_collision_probability": estimated_pc,
                "estimated_sigma_radial_km_primary": sigmas[i][0],
                "estimated_sigma_intrack_km_primary": sigmas[i][1],
                "estimated_sigma_crosstrack_km_primary": sigmas[i][2],
                "estimated_sigma_radial_km_secondary": sigmas[j][0],
                "estimated_sigma_intrack_km_secondary": sigmas[j][1],
                "estimated_sigma_crosstrack_km_secondary": sigmas[j][2],
                "covariance_source": covariance_source,
                "hard_body_radius_km": hard_body_radius_km,
            }
        )

    summary = {
        "screening_run_id": run_id,
        "started_at": started.isoformat(),
        "as_of": as_of.isoformat() if as_of else None,
        "is_backfill": as_of is not None,
        "completed_at": datetime.now(timezone.utc).isoformat(),
        "window_start": window_start.isoformat(),
        "window_hours": window_hours,
        "coarse_step_s": coarse_step_s,
        "coarse_radius_km": coarse_radius_km,
        "miss_threshold_km": miss_threshold_km,
        "fine_step_s": fine_step_s,
        "hard_body_radius_km": hard_body_radius_km,
        # Coverage facts. These exist so a partial screen can never be mistaken
        # for a complete one downstream.
        "objects_screened": len(sats),
        "objects_failed_init": init_failures,
        "candidate_pairs": len(candidates),
        "conjunctions_found": len(events),
        # Co-located/docked pairs suppressed. Recorded rather than silently
        # dropped: a jump here means the catalogue gained duplicate entries.
        "element_set_twins_suppressed": element_set_twins,
        "candidate_cap_hit": len(candidates) >= MAX_CANDIDATE_PAIRS,
        "catalogue_sources": ",".join(sorted({m["catalogue_source"] for m in meta})),
    }

    return summary, events


@dlt.source(name="conjunction_screening")
def conjunction_screening_source(dsn: dict, **screen_kwargs: Any) -> Any:
    """One screening run: a summary row plus the conjunctions it found."""
    # Backfilling means moving the whole screen into the past, not just its
    # window. Defaulting as_of to epoch_start makes that the automatic
    # behaviour, so a caller cannot accidentally screen a past window using
    # today's orbits — the one mistake here that produces plausible-looking
    # nonsense rather than an error. An explicit as_of still wins.
    if screen_kwargs.get("epoch_start") is not None and screen_kwargs.get("as_of") is None:
        screen_kwargs["as_of"] = screen_kwargs["epoch_start"]

    summary, events = screen_catalogue(dsn, **screen_kwargs)

    @dlt.resource(
        name="screening_run", write_disposition="merge", primary_key="screening_run_id"
    )
    def screening_run() -> Iterator[dict]:
        yield summary

    @dlt.resource(
        name="conjunction_event",
        write_disposition="merge",
        # One row per pair per run. A pair legitimately reappears in later runs
        # with a refined TCA, so the run id is part of the key.
        primary_key=[
            "screening_run_id",
            "primary_norad_cat_id",
            "secondary_norad_cat_id",
        ],
    )
    def conjunction_event() -> Iterator[dict]:
        yield from events

    return [screening_run, conjunction_event]


def load_conjunction_screening(
    credentials: Optional[dict] = None,
    dev_mode: bool = False,
    destination_override: Optional[Any] = None,
    **screen_kwargs: Any,
) -> str:
    """Run a screen and load its results into the Postgres schema `raw`.

    Args:
        credentials: Postgres connection as a dict of database/username/
            password/host/port. Required — this pipeline READS the warehouse as
            well as writing to it, so unlike the ingestion pipelines there is no
            meaningful secrets.toml-only path.
        dev_mode: Load into a fresh timestamped dataset instead of `raw`.
        destination_override: A dlt destination to use instead of Postgres.
        **screen_kwargs: Passed to screen_catalogue — window_hours,
            coarse_step_s, coarse_radius_km, miss_threshold_km, max_objects.

    Returns:
        The dlt load info, stringified.
    """
    if not credentials:
        raise ValueError(
            "conjunction screening needs warehouse credentials: it reads the "
            "staged catalogue from analytics before it can write anything."
        )

    dsn = {
        "dbname": credentials["database"],
        "user": credentials["username"],
        "password": credentials["password"],
        "host": credentials["host"],
        "port": credentials.get("port", 5432),
    }

    if destination_override is not None:
        destination: Any = destination_override
    else:
        destination = dlt.destinations.postgres(credentials=credentials)

    pipeline = dlt.pipeline(
        pipeline_name="conjunction_screening",
        destination=destination,
        dataset_name="raw",
        pipelines_dir=_state_dir(),
        dev_mode=dev_mode,
    )

    info = pipeline.run(conjunction_screening_source(dsn, **screen_kwargs))
    return str(info)
