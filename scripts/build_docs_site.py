"""Assemble the public documentation site published to GitHub Pages.

The site has two halves and a landing page that ties them together:

    site/index.html       landing page — what this platform is, where to go
    site/dbt/index.html   dbt docs (dbt's own static single-file build)
    site/cube/index.html  Cube semantic layer reference, generated from
                          semantic/cubes/*.yml

Why the Cube half is generated from the REPO rather than from Cube's live
`/cubejs-api/v1/meta` endpoint: the endpoint only exists on 10.0.0.50, a LAN
address a GitHub-hosted runner cannot reach. Reading the checked-in YAML also
keeps the published page honest about what is *committed* rather than what
happens to be loaded in a long-running container.

The generator additionally cross-checks every cube's `sql_table` against the
dbt manifest. `semantic/README.md` names that drift — "rename a column in the
marts and the cube breaks" — as the reason the two directories live in one
repo. Until now nothing checked it; now CI does, and any mismatch is rendered
on the page and emitted as a GitHub Actions warning. It is deliberately NOT a
build failure: a missing mart should degrade the docs, not block publishing
them. Flip `--strict` on if that trade stops being the right one.
"""

from __future__ import annotations

import argparse
import html
import json
import os
import shutil
import sys
from pathlib import Path
from typing import Any

import yaml

# dbt docs routes entirely in the URL fragment, so a model page is reachable
# without a server. This is what lets the Cube reference deep-link into the
# dbt docs for the mart a cube is built on.
#
# The fragment takes the node's `unique_id` straight from the manifest rather
# than rebuilding it as "model.<project>.<name>". The two are identical today;
# taking dbt's own value means renaming the dbt project cannot quietly turn
# every one of these links into a 404.
DBT_MODEL_FRAGMENT = "../dbt/index.html#!/model/{unique_id}"


def load_cubes(cube_dir: Path) -> list[dict[str, Any]]:
    """Read every cube definition, in a stable order.

    Sorted by filename so the generated HTML is byte-identical between runs
    with no input change — a churning diff would make the Pages deploy history
    useless for spotting real edits.
    """
    cubes: list[dict[str, Any]] = []
    for path in sorted(cube_dir.glob("*.yml")):
        doc = yaml.safe_load(path.read_text()) or {}
        for cube in doc.get("cubes", []):
            cube["_source_file"] = path.name
            cubes.append(cube)
    return cubes


def mart_models(manifest_path: Path) -> dict[str, dict[str, Any]]:
    """Map `analytics.<name>` -> the dbt model node that produces it.

    Keyed on the schema-qualified name because that is exactly the string a
    cube puts in `sql_table`, so the comparison needs no normalising.
    """
    manifest = json.loads(manifest_path.read_text())
    models: dict[str, dict[str, Any]] = {}
    for node in manifest.get("nodes", {}).values():
        if node.get("resource_type") != "model":
            continue
        models[f"{node['schema']}.{node['name']}"] = node
    return models


def esc(value: Any) -> str:
    """HTML-escape, collapsing the folded-scalar newlines YAML leaves behind."""
    if value is None:
        return ""
    return html.escape(" ".join(str(value).split()))


def render_members(members: list[dict[str, Any]], kind: str) -> str:
    """Render the dimension or measure table for one cube."""
    if not members:
        return f'<p class="empty">No {kind} defined.</p>'

    rows = []
    for m in members:
        name = esc(m.get("name"))
        # A measure's aggregation (count, sum, min…) and a dimension's data
        # type both live in `type`, so one column serves both.
        badges = [f'<span class="badge">{esc(m.get("type"))}</span>']
        if m.get("primary_key"):
            badges.append('<span class="badge pk">primary key</span>')
        if m.get("filters"):
            # A filtered measure is not what its name suggests it is —
            # `real_conjunction_count` is a count with intra-constellation
            # pairs removed. Surfacing the predicate stops a dashboard author
            # from assuming it is a plain count.
            for f in m["filters"]:
                badges.append(f'<span class="badge filter">where {esc(f.get("sql"))}</span>')

        title = esc(m.get("title")) or name
        desc = esc(m.get("description"))

        # Show the underlying expression only when it is not simply the member
        # name again. Two thirds of these are a plain passthrough, and printing
        # `retry_reason -> retry_reason` 80 times buries the ~35 that carry
        # real information: a rename (`rows_moved` <- `row_count`), a dlt
        # internal column (`dlt_load_id` <- `_dlt_load_id`), or a derived
        # expression. Those are exactly what someone querying Cube needs to
        # map back onto the warehouse.
        sql_raw = m.get("sql")
        sql = esc(sql_raw) if sql_raw and sql_raw != m.get("name") else ""

        rows.append(
            f"""      <tr>
        <td><code>{name}</code>{f'<div class="sub">{title}</div>' if title != name else ""}</td>
        <td>{" ".join(badges)}</td>
        <td>{desc or '<span class="empty">—</span>'}{f'<div class="sub">from <code>{sql}</code></div>' if sql else ""}</td>
      </tr>"""
        )

    return f"""    <table>
      <thead><tr><th>Name</th><th>Type</th><th>Description</th></tr></thead>
      <tbody>
{chr(10).join(rows)}
      </tbody>
    </table>"""


def render_cube(cube: dict[str, Any], marts: dict[str, dict[str, Any]]) -> tuple[str, str | None]:
    """Render one cube section. Returns (html, drift_warning_or_None)."""
    name = esc(cube.get("name"))
    sql_table = cube.get("sql_table", "")
    node = marts.get(sql_table)

    warning = None
    if node is None:
        warning = (
            f"cube '{cube.get('name')}' ({cube['_source_file']}) reads "
            f"'{sql_table}', which is not a model in the dbt project"
        )
        mart_link = (
            f'<code>{esc(sql_table)}</code> '
            f'<span class="badge drift">not found in dbt project</span>'
        )
    else:
        mart_link = (
            f'<a href="{DBT_MODEL_FRAGMENT.format(unique_id=node["unique_id"])}">'
            f"<code>{esc(sql_table)}</code></a>"
        )

    dims = cube.get("dimensions", [])
    measures = cube.get("measures", [])

    extras = []
    if cube.get("joins"):
        extras.append(f'{len(cube["joins"])} join(s)')
    if cube.get("pre_aggregations"):
        extras.append(f'{len(cube["pre_aggregations"])} pre-aggregation(s)')
    extra_html = (
        f'<p class="extras">Also defines {", ".join(extras)} — see '
        f'<code>semantic/cubes/{esc(cube["_source_file"])}</code>.</p>'
        if extras
        else ""
    )

    return (
        f"""  <section class="cube" id="{name}">
    <h2>{esc(cube.get("title")) or name}</h2>
    <p class="cubename"><code>{name}</code> &middot; built on {mart_link}
      &middot; defined in <code>semantic/cubes/{esc(cube["_source_file"])}</code></p>
    <p class="desc">{esc(cube.get("description"))}</p>
    {extra_html}
    <h3>Dimensions <span class="count">{len(dims)}</span></h3>
{render_members(dims, "dimensions")}
    <h3>Measures <span class="count">{len(measures)}</span></h3>
{render_members(measures, "measures")}
  </section>""",
        warning,
    )


STYLE = """
:root {
  --bg: #ffffff; --fg: #1a1d21; --muted: #5c6570; --line: #e2e6ea;
  --accent: #1f6feb; --code-bg: #f4f6f8; --badge: #eef1f4; --warn: #b45309;
  --warn-bg: #fff7ed;
}
@media (prefers-color-scheme: dark) {
  :root {
    --bg: #0d1117; --fg: #e6edf3; --muted: #9198a1; --line: #2a3038;
    --accent: #58a6ff; --code-bg: #161b22; --badge: #1f242c; --warn: #f0b429;
    --warn-bg: #2a2114;
  }
}
* { box-sizing: border-box; }
body {
  margin: 0; padding: 2rem 1.25rem 5rem; background: var(--bg); color: var(--fg);
  font: 15px/1.6 -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
}
.wrap { max-width: 60rem; margin: 0 auto; }
h1 { font-size: 1.9rem; margin: 0 0 .35rem; letter-spacing: -.02em; }
h2 { font-size: 1.35rem; margin: 0 0 .3rem; letter-spacing: -.01em; }
h3 { font-size: .8rem; text-transform: uppercase; letter-spacing: .08em;
     color: var(--muted); margin: 1.6rem 0 .5rem; }
a { color: var(--accent); }
code { background: var(--code-bg); padding: .12em .38em; border-radius: 4px;
       font: 13px/1.5 ui-monospace, SFMono-Regular, Menlo, monospace; }
.lede { color: var(--muted); margin: 0 0 2rem; }
.cube { border-top: 1px solid var(--line); padding-top: 1.8rem; margin-top: 2.4rem; }
.cubename, .desc, .extras { margin: .25rem 0; }
.cubename { color: var(--muted); font-size: .88rem; }
.desc { margin-bottom: .5rem; }
.extras { color: var(--muted); font-size: .88rem; }
.count { background: var(--badge); color: var(--muted); border-radius: 10px;
         padding: .05em .5em; font-size: .8rem; letter-spacing: 0; }
table { width: 100%; border-collapse: collapse; font-size: .9rem; }
th { text-align: left; font-size: .72rem; text-transform: uppercase;
     letter-spacing: .06em; color: var(--muted); font-weight: 600;
     border-bottom: 1px solid var(--line); padding: .4rem .6rem .4rem 0; }
td { border-bottom: 1px solid var(--line); padding: .55rem .6rem .55rem 0;
     vertical-align: top; }
td:first-child { width: 30%; } td:nth-child(2) { width: 22%; }
.sub { color: var(--muted); font-size: .84rem; margin-top: .2rem; }
.badge { display: inline-block; background: var(--badge); color: var(--muted);
         border-radius: 4px; padding: .1em .45em; font-size: .78rem;
         margin: 0 .25rem .25rem 0;
         font-family: ui-monospace, SFMono-Regular, Menlo, monospace; }
.badge.pk { color: var(--accent); }
.badge.filter { white-space: normal; }
.badge.drift { color: var(--warn); }
.empty { color: var(--muted); }
.drift-note { background: var(--warn-bg); border-left: 3px solid var(--warn);
              padding: .8rem 1rem; border-radius: 0 6px 6px 0; margin: 1.5rem 0; }
.drift-note strong { color: var(--warn); }
.toc { display: flex; flex-wrap: wrap; gap: .5rem; margin: 0 0 1rem; padding: 0;
       list-style: none; }
.toc a { display: inline-block; background: var(--badge); border-radius: 6px;
         padding: .3rem .6rem; text-decoration: none; font-size: .88rem; }
.cards { display: grid; gap: 1rem; grid-template-columns: repeat(auto-fit, minmax(15rem, 1fr));
         margin: 2rem 0; }
.card { border: 1px solid var(--line); border-radius: 10px; padding: 1.2rem;
        text-decoration: none; color: inherit; display: block; }
.card:hover { border-color: var(--accent); }
.card h2 { font-size: 1.05rem; margin-bottom: .3rem; }
.card p { margin: 0; color: var(--muted); font-size: .9rem; }
.meta { color: var(--muted); font-size: .82rem; border-top: 1px solid var(--line);
        margin-top: 3rem; padding-top: 1rem; }
.caveat { background: var(--warn-bg); border-left: 3px solid var(--warn);
          padding: .8rem 1rem; border-radius: 0 6px 6px 0; margin: 1.5rem 0;
          font-size: .9rem; }
"""


def page(title: str, body: str) -> str:
    return f"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>{html.escape(title)}</title>
<style>{STYLE}</style>
</head>
<body><div class="wrap">
{body}
</div></body>
</html>
"""


def build_cube_page(cubes: list[dict[str, Any]], marts: dict[str, dict[str, Any]]) -> tuple[str, list[str]]:
    sections, warnings = [], []
    for cube in cubes:
        section, warning = render_cube(cube, marts)
        sections.append(section)
        if warning:
            warnings.append(warning)

    toc = "".join(
        f'<li><a href="#{esc(c.get("name"))}">{esc(c.get("title")) or esc(c.get("name"))}</a></li>'
        for c in cubes
    )

    drift = ""
    if warnings:
        items = "".join(f"<li>{html.escape(w)}</li>" for w in warnings)
        drift = (
            f'<div class="drift-note"><strong>Model drift detected.</strong> '
            f"A cube names a table the dbt project does not build. The cube and the "
            f"mart it reads are versioned together precisely so this cannot go "
            f"unnoticed:<ul>{items}</ul></div>"
        )

    n_dims = sum(len(c.get("dimensions", [])) for c in cubes)
    n_measures = sum(len(c.get("measures", [])) for c in cubes)

    body = f"""<p><a href="../index.html">&larr; Documentation home</a></p>
<h1>Cube semantic layer</h1>
<p class="lede">{len(cubes)} cubes &middot; {n_dims} dimensions &middot; {n_measures} measures.
Generated from <code>semantic/cubes/</code>. Every cube is defined over a dbt
<em>mart</em> — never over a raw table — so the casts and derived flags it
exposes are the ones dbt tests. Each cube links through to the model that
builds it.</p>
{drift}
<ul class="toc">{toc}</ul>
{chr(10).join(sections)}
"""
    return page("Cube semantic layer — Norion Analytics", body), warnings


def build_landing(cubes: list[dict[str, Any]], manifest_path: Path, has_catalog: bool) -> str:
    manifest = json.loads(manifest_path.read_text())
    nodes = manifest.get("nodes", {})
    models = [n for n in nodes.values() if n.get("resource_type") == "model"]
    tests = [n for n in nodes.values() if n.get("resource_type") == "test"]
    sources = manifest.get("sources", {})
    generated = manifest.get("metadata", {}).get("generated_at", "")

    # Column types come from the warehouse catalog, which CI cannot query.
    # Saying so on the page is cheaper than fielding the question.
    catalog_note = (
        ""
        if has_catalog
        else """<div class="caveat"><strong>Built without a warehouse connection.</strong>
The runner that publishes this site has no route to the warehouse, so the docs
carry the model graph, descriptions, tests and source SQL, but not
warehouse-derived column data types or table statistics. Column documentation
below comes from the project's YAML, which is the authored source of truth
either way.</div>"""
    )

    return page(
        "Norion Analytics — data platform documentation",
        f"""<h1>Norion Analytics</h1>
<p class="lede">Documentation for the space-domain-awareness data warehouse:
NASA APOD, DONKI space weather and NEO feeds, CelesTrak and Space-Track
catalogues, and an SGP4 conjunction screen — modelled in dbt, served through
Cube.</p>
{catalog_note}
<div class="cards">
  <a class="card" href="dbt/index.html">
    <h2>dbt project &rarr;</h2>
    <p>{len(models)} models, {len(sources)} sources, {len(tests)} tests.
       Lineage graph, column documentation and the SQL behind every table.</p>
  </a>
  <a class="card" href="cube/index.html">
    <h2>Semantic layer &rarr;</h2>
    <p>{len(cubes)} cubes. The dimensions and measures available for querying,
       each linked to the dbt mart it is built on.</p>
  </a>
</div>
<h3>How to read this</h3>
<p>Raw API payloads land in the <code>raw</code> schema via dlt and are left
exactly as the provider sent them. <code>stg_</code> models rename and cast
that layer 1:1. Marts are the business-facing tables, and are the only thing
Cube is permitted to read.</p>
<div class="caveat"><strong>On the conjunction screen.</strong> Collision
probabilities in <code>conjunction_events</code> are <em>estimated</em> from
covariance derived from TLE scatter, because TLEs carry none of their own. They
rank what deserves attention. They are not a substitute for a CDM and must not
drive a manoeuvre decision — which is why every such field keeps
<code>estimated</code> in its name.</div>
<p class="meta">dbt project parsed {html.escape(generated)}.
Published from <code>tbar4/norion-analytics</code>.</p>""",
    )


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--target", required=True, type=Path, help="dbt target/ dir")
    ap.add_argument("--cubes", required=True, type=Path, help="semantic/cubes/ dir")
    ap.add_argument("--out", required=True, type=Path, help="site output dir")
    ap.add_argument(
        "--strict",
        action="store_true",
        help="exit non-zero if a cube references a table dbt does not build",
    )
    args = ap.parse_args()

    manifest_path = args.target / "manifest.json"
    if not manifest_path.exists():
        print(f"::error::no manifest at {manifest_path} — run `dbt parse` first", file=sys.stderr)
        return 1

    # dbt writes the single-file build as static_index.html. Serving it as
    # index.html is what makes /dbt/ a working directory URL on Pages.
    static_index = args.target / "static_index.html"
    if not static_index.exists():
        print(f"::error::no static_index.html at {static_index} — pass --static", file=sys.stderr)
        return 1

    out_dbt = args.out / "dbt"
    out_cube = args.out / "cube"
    for d in (out_dbt, out_cube):
        d.mkdir(parents=True, exist_ok=True)

    shutil.copy2(static_index, out_dbt / "index.html")

    # Ship the artifacts too: they are the machine-readable form of everything
    # on this site, and anyone wanting to diff the graph or drive a catalog
    # ingestion wants these rather than the HTML.
    has_catalog = False
    for artifact in ("manifest.json", "catalog.json"):
        src = args.target / artifact
        if src.exists():
            shutil.copy2(src, out_dbt / artifact)
    catalog_path = args.target / "catalog.json"
    if catalog_path.exists():
        catalog = json.loads(catalog_path.read_text())
        has_catalog = bool(catalog.get("nodes"))

    cubes = load_cubes(args.cubes)
    marts = mart_models(manifest_path)

    cube_html, warnings = build_cube_page(cubes, marts)
    (out_cube / "index.html").write_text(cube_html)
    (args.out / "index.html").write_text(build_landing(cubes, manifest_path, has_catalog))

    # .nojekyll stops Pages' Jekyll pass from dropping files whose names begin
    # with an underscore. dbt's static build does not currently emit any, but
    # the cost of being wrong later is a silently broken page.
    (args.out / ".nojekyll").write_text("")

    for w in warnings:
        print(f"::warning::{w}")

    print(
        f"built site: {len(marts)} dbt models, {len(cubes)} cubes, "
        f"catalog={'populated' if has_catalog else 'empty'}, "
        f"{len(warnings)} drift warning(s)"
    )

    if warnings and args.strict:
        print("::error::cube/mart drift, and --strict is set", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
