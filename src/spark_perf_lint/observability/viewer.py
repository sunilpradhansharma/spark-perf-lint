"""Static HTML trace report generator for spark-perf-lint.

Reads all ``spl-trace-*.json`` files from a directory and produces a
single, fully self-contained HTML file (no CDN, no external assets).

Report sections
---------------
1. **Summary cards** — total runs, aggregate finding counts, pass rate.
2. **Trend chart** — SVG line chart of CRITICAL / WARNING / INFO findings
   over time across all loaded traces.
3. **Run history table** — one row per trace: timestamp, duration, files
   scanned, per-severity counts, pass/fail badge.
4. **Dimension breakdown** — aggregate findings by Spark dimension across
   all traces, rendered as a horizontal bar chart.
5. **Hotspot files** — top-10 files ranked by total findings across all
   traces that include ``file_stats`` (standard/verbose trace level).

Usage as a library::

    from pathlib import Path
    from spark_perf_lint.observability.viewer import TraceViewer

    viewer = TraceViewer(Path(".spark-perf-lint-traces"))
    html   = viewer.render_html()
    Path("report.html").write_text(html, encoding="utf-8")

CLI::

    spark-perf-lint traces --dir .spark-perf-lint-traces --output report.html
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

# Human-readable labels for each dimension code.
_DIMENSION_LABELS: dict[str, str] = {
    "D01_CLUSTER_CONFIG": "D01 · Cluster Config",
    "D02_SHUFFLE": "D02 · Shuffle",
    "D03_JOINS": "D03 · Joins",
    "D04_PARTITIONING": "D04 · Partitioning",
    "D05_SKEW": "D05 · Data Skew",
    "D06_CACHING": "D06 · Caching",
    "D07_IO_FORMAT": "D07 · I/O Format",
    "D08_AQE": "D08 · AQE",
    "D09_UDF_CODE": "D09 · UDF Code",
    "D10_CATALYST": "D10 · Catalyst",
    "D11_MONITORING": "D11 · Monitoring",
}


# =============================================================================
# Trace loading
# =============================================================================


def _load_one_trace(path: Path) -> dict[str, Any] | None:
    """Parse a single trace JSON file into a normalised record.

    Missing or malformed keys are filled with zero/empty defaults so the
    renderer never needs to guard against ``KeyError``.

    Args:
        path: Path to a ``spl-trace-*.json`` file.

    Returns:
        A normalised dict, or ``None`` when the file cannot be read or
        parsed.
    """
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        logger.warning("TraceViewer: skipping %s — %s", path.name, exc)
        return None

    run = raw.get("run", {})
    summary = raw.get("summary", {})
    by_sev = summary.get("by_severity", {})
    by_dim = summary.get("by_dimension", {})

    # Normalise started_at to an ISO string for JS Date parsing
    started_at = run.get("started_at", "")

    return {
        "run_id": run.get("run_id", path.stem),
        "started_at": started_at,
        "ended_at": run.get("ended_at", ""),
        "duration_s": float(run.get("duration_s", 0.0)),
        "files_scanned": int(run.get("files_scanned", 0)),
        "version": run.get("spark_perf_lint_version", "?"),
        "total": int(summary.get("total", 0)),
        "critical": int(by_sev.get("CRITICAL", 0)),
        "warning": int(by_sev.get("WARNING", 0)),
        "info": int(by_sev.get("INFO", 0)),
        "by_dimension": {k: int(v) for k, v in by_dim.items()},
        "file_stats": raw.get("file_stats", []),
        "passed": raw.get("run", {}).get("passed", None),  # may be absent
        "source_file": path.name,
    }


# =============================================================================
# TraceViewer
# =============================================================================


class TraceViewer:
    """Loads trace files and renders a self-contained HTML report.

    Args:
        trace_dir: Directory containing ``spl-trace-*.json`` files.
    """

    def __init__(self, trace_dir: Path) -> None:
        self._trace_dir = trace_dir

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def load_traces(self) -> list[dict[str, Any]]:
        """Discover and parse all trace files in *trace_dir*.

        Files are sorted chronologically by ``started_at``.  Files that
        cannot be parsed are skipped with a warning.

        Returns:
            List of normalised trace dicts, oldest first.
        """
        pattern = "spl-trace-*.json"
        paths = sorted(self._trace_dir.glob(pattern))
        if not paths:
            logger.info("TraceViewer: no trace files found in %s", self._trace_dir)
            return []

        traces = [t for p in paths if (t := _load_one_trace(p)) is not None]
        # Sort by started_at string — ISO 8601 sorts lexicographically
        traces.sort(key=lambda t: t["started_at"])
        logger.info("TraceViewer: loaded %d trace(s) from %s", len(traces), self._trace_dir)
        return traces

    def render_html(self, title: str = "spark-perf-lint Trace Report") -> str:
        """Load all traces and render the full HTML report.

        Args:
            title: The ``<title>`` and ``<h1>`` used in the generated page.

        Returns:
            A complete, self-contained UTF-8 HTML string.
        """
        traces = self.load_traces()
        return _render_html(traces, title=title, trace_dir=str(self._trace_dir))


# =============================================================================
# HTML rendering
# =============================================================================


def _render_html(
    traces: list[dict[str, Any]],
    title: str,
    trace_dir: str,
) -> str:
    """Build the full HTML document from *traces*.

    Args:
        traces: Normalised trace dicts (oldest first).
        title: Page title string.
        trace_dir: Original trace directory path (shown in the header).

    Returns:
        Complete HTML document as a string.
    """
    generated_at = datetime.now(tz=timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    # Aggregate totals for summary cards
    n_runs = len(traces)
    total_findings = sum(t["total"] for t in traces)
    total_critical = sum(t["critical"] for t in traces)
    pass_count = sum(1 for t in traces if t["critical"] == 0)
    pass_rate = f"{100 * pass_count // n_runs}%" if n_runs else "—"

    # Aggregate by-dimension across all traces
    dim_totals: dict[str, int] = {}
    for trace in traces:
        for dim, count in trace["by_dimension"].items():
            dim_totals[dim] = dim_totals.get(dim, 0) + count
    dim_totals_sorted = sorted(dim_totals.items(), key=lambda x: x[1], reverse=True)

    # Aggregate hotspot files
    file_totals: dict[str, int] = {}
    for trace in traces:
        for fs in trace.get("file_stats", []):
            fp = fs.get("file", "")
            file_totals[fp] = file_totals.get(fp, 0) + int(fs.get("findings", 0))
    hotspot_files = sorted(file_totals.items(), key=lambda x: x[1], reverse=True)[:10]

    # JSON payload for the inline JS
    data_json = json.dumps(traces, indent=None, default=str)

    # Build the dimension table rows
    dim_max = dim_totals_sorted[0][1] if dim_totals_sorted else 1
    dim_rows = ""
    for dim_key, count in dim_totals_sorted:
        label = _DIMENSION_LABELS.get(dim_key, dim_key)
        pct = int(100 * count / max(dim_max, 1))
        dim_rows += (
            f'<tr><td class="dim-label">{_esc(label)}</td>'
            f'<td><div class="bar-wrap"><div class="bar" style="width:{pct}%"></div></div></td>'
            f'<td class="dim-count">{count}</td></tr>\n'
        )

    # Build the hotspot files rows
    file_rows = ""
    file_max = hotspot_files[0][1] if hotspot_files else 1
    for fp, count in hotspot_files:
        pct = int(100 * count / max(file_max, 1))
        short = fp.split("/")[-1] if "/" in fp else fp
        file_rows += (
            f"<tr>"
            f'<td class="file-path" title="{_esc(fp)}">{_esc(short)}</td>'
            f'<td><div class="bar-wrap"><div class="bar bar-file" style="width:{pct}%"></div></div></td>'  # noqa: E501
            f'<td class="dim-count">{count}</td>'
            f"</tr>\n"
        )
    if not file_rows:
        file_rows = '<tr><td colspan="3" class="empty-row">No file stats available (requires trace_level: standard or verbose)</td></tr>'  # noqa: E501

    if not dim_rows:
        dim_rows = '<tr><td colspan="3" class="empty-row">No findings across all traces</td></tr>'

    no_traces_banner = (
        ""
        if traces
        else (
            '<div class="no-traces">No trace files found in <code>'
            + _esc(trace_dir)
            + "</code>. Enable observability and run a scan first.</div>"
        )
    )

    return _HTML_TEMPLATE.format(
        title=_esc(title),
        generated_at=generated_at,
        trace_dir=_esc(trace_dir),
        n_runs=n_runs,
        total_findings=total_findings,
        total_critical=total_critical,
        pass_rate=pass_rate,
        dim_rows=dim_rows,
        file_rows=file_rows,
        no_traces_banner=no_traces_banner,
        data_json=data_json,
    )


def _esc(text: str) -> str:
    """Minimal HTML-escape for text inserted into HTML attributes and text nodes.

    Only escapes the five characters that break HTML structure.

    Args:
        text: Raw string to escape.

    Returns:
        HTML-safe string.
    """
    return (
        str(text)
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
        .replace("'", "&#39;")
    )


# =============================================================================
# Convenience function
# =============================================================================


def generate_report(
    trace_dir: Path, output_path: Path, title: str = "spark-perf-lint Trace Report"
) -> int:
    """Load traces from *trace_dir* and write an HTML report to *output_path*.

    Args:
        trace_dir: Directory containing ``spl-trace-*.json`` files.
        output_path: Destination HTML file path.
        title: Report title shown in the page header.

    Returns:
        Number of trace files that were loaded and included in the report.
    """
    viewer = TraceViewer(trace_dir)
    traces = viewer.load_traces()
    html = _render_html(traces, title=title, trace_dir=str(trace_dir))

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(html, encoding="utf-8")
    return len(traces)


# =============================================================================
# HTML template — inlines all CSS and JS; no external dependencies
# =============================================================================

_HTML_TEMPLATE = """\
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>{title}</title>
<style>
*, *::before, *::after {{ box-sizing: border-box; margin: 0; padding: 0; }}

:root {{
  --bg:        #0f1117;
  --surface:   #1a1d27;
  --border:    #2a2d3a;
  --text:      #e2e6f0;
  --muted:     #7a7f99;
  --accent:    #4f8ef7;
  --critical:  #f75050;
  --warning:   #f7b050;
  --info:      #50b8f7;
  --pass:      #50c878;
  --fail:      #f75050;
  --bar-dim:   #4f8ef7;
  --bar-file:  #9b59f7;
  font-family: 'Segoe UI', system-ui, -apple-system, sans-serif;
  font-size: 14px;
  color: var(--text);
  background: var(--bg);
}}

body {{ padding: 24px; max-width: 1200px; margin: 0 auto; }}

h1 {{ font-size: 1.5rem; font-weight: 700; color: var(--accent); margin-bottom: 4px; }}
h2 {{ font-size: 1.05rem; font-weight: 600; color: var(--text); margin: 28px 0 12px; }}
.subtitle {{ color: var(--muted); font-size: 0.85rem; margin-bottom: 28px; }}

/* ── Cards ── */
.cards {{ display: flex; gap: 16px; flex-wrap: wrap; margin-bottom: 28px; }}
.card {{
  background: var(--surface); border: 1px solid var(--border);
  border-radius: 10px; padding: 16px 22px; min-width: 130px; flex: 1;
}}
.card-label {{ font-size: 0.75rem; text-transform: uppercase;
               letter-spacing: .07em; color: var(--muted); margin-bottom: 6px; }}
.card-value {{ font-size: 1.9rem; font-weight: 700; line-height: 1; }}
.card-value.c {{ color: var(--critical); }}
.card-value.w {{ color: var(--warning); }}
.card-value.p {{ color: var(--pass); }}

/* ── Section panels ── */
.panel {{
  background: var(--surface); border: 1px solid var(--border);
  border-radius: 10px; padding: 20px 24px; margin-bottom: 24px;
}}

/* ── Trend chart ── */
#trend-svg {{ width: 100%; height: 180px; display: block; }}
.trend-legend {{ display: flex; gap: 20px; margin-top: 10px;
               font-size: 0.8rem; color: var(--muted); }}
.legend-dot {{ display: inline-block; width: 10px; height: 10px;
               border-radius: 50%; margin-right: 5px; }}
.no-trend {{ color: var(--muted); font-size: 0.85rem; padding: 20px 0; text-align: center; }}

/* ── Run history table ── */
.run-table {{ width: 100%; border-collapse: collapse; font-size: 0.85rem; }}
.run-table th {{
  text-align: left; padding: 8px 12px; font-weight: 600;
  color: var(--muted); border-bottom: 1px solid var(--border);
  font-size: 0.75rem; text-transform: uppercase; letter-spacing: .05em;
}}
.run-table td {{ padding: 7px 12px; border-bottom: 1px solid var(--border); }}
.run-table tr:last-child td {{ border-bottom: none; }}
.run-table tr:hover td {{ background: rgba(79,142,247,.06); }}
.badge {{
  display: inline-block; padding: 2px 9px; border-radius: 99px;
  font-size: 0.72rem; font-weight: 600; letter-spacing: .04em;
}}
.badge-pass {{ background: rgba(80,200,120,.15); color: var(--pass); }}
.badge-fail {{ background: rgba(247,80,80,.15); color: var(--fail); }}
.badge-unknown {{ background: rgba(122,127,153,.15); color: var(--muted); }}
.sev-c {{ color: var(--critical); font-weight: 600; }}
.sev-w {{ color: var(--warning); font-weight: 600; }}
.sev-i {{ color: var(--info); }}
.run-id-short {{ font-family: monospace; color: var(--muted); font-size: 0.78rem; }}
.empty-table {{ color: var(--muted); font-size: 0.85rem;
                padding: 24px; text-align: center; display: block; }}

/* ── Dimension + hotspot bars ── */
.bar-table {{ width: 100%; border-collapse: collapse; font-size: 0.85rem; }}
.bar-table td {{ padding: 5px 8px; vertical-align: middle; }}
.dim-label {{ color: var(--text); width: 180px; white-space: nowrap; }}
.file-path {{ color: var(--text); max-width: 240px;
              overflow: hidden; text-overflow: ellipsis; white-space: nowrap;
              font-family: monospace; font-size: 0.8rem; }}
.bar-wrap {{ background: var(--border); border-radius: 4px; height: 16px; min-width: 40px; }}
.bar {{ height: 100%; border-radius: 4px; background: var(--bar-dim); min-width: 2px; }}
.bar-file {{ background: var(--bar-file); }}
.dim-count {{ color: var(--muted); text-align: right; width: 48px;
             font-variant-numeric: tabular-nums; }}
.empty-row {{ color: var(--muted); font-size: 0.85rem; padding: 16px 0; text-align: center; }}

/* ── Layouts ── */
.two-col {{ display: grid; grid-template-columns: 1fr 1fr; gap: 20px; }}
@media (max-width: 720px) {{ .two-col {{ grid-template-columns: 1fr; }} }}

.no-traces {{
  background: rgba(247,176,80,.08); border: 1px solid rgba(247,176,80,.3);
  border-radius: 8px; padding: 16px 20px; color: var(--warning); margin-bottom: 20px;
  font-size: 0.9rem;
}}
</style>
</head>
<body>

<h1>{title}</h1>
<p class="subtitle">Generated {generated_at} &nbsp;·&nbsp; Source: <code>{trace_dir}</code></p>

{no_traces_banner}

<div class="cards">
  <div class="card">
    <div class="card-label">Runs</div>
    <div class="card-value">{n_runs}</div>
  </div>
  <div class="card">
    <div class="card-label">Total Findings</div>
    <div class="card-value w">{total_findings}</div>
  </div>
  <div class="card">
    <div class="card-label">Critical</div>
    <div class="card-value c">{total_critical}</div>
  </div>
  <div class="card">
    <div class="card-label">Pass Rate</div>
    <div class="card-value p">{pass_rate}</div>
  </div>
</div>

<h2>Findings Trend</h2>
<div class="panel">
  <svg id="trend-svg" aria-label="Findings trend chart"></svg>
  <div class="trend-legend">
    <span><span class="legend-dot" style="background:#f75050"></span>Critical</span>
    <span><span class="legend-dot" style="background:#f7b050"></span>Warning</span>
    <span><span class="legend-dot" style="background:#50b8f7"></span>Info</span>
  </div>
</div>

<h2>Run History</h2>
<div class="panel">
  <table class="run-table" id="run-table">
    <thead>
      <tr>
        <th>Timestamp</th>
        <th>Run ID</th>
        <th>Duration</th>
        <th>Files</th>
        <th>Critical</th>
        <th>Warning</th>
        <th>Info</th>
        <th>Status</th>
      </tr>
    </thead>
    <tbody id="run-tbody"></tbody>
  </table>
</div>

<div class="two-col">
  <div>
    <h2>Dimension Breakdown</h2>
    <div class="panel">
      <table class="bar-table">
        <tbody>{dim_rows}</tbody>
      </table>
    </div>
  </div>
  <div>
    <h2>Hotspot Files</h2>
    <div class="panel">
      <table class="bar-table">
        <tbody>{file_rows}</tbody>
      </table>
    </div>
  </div>
</div>

<script>
(function () {{
  "use strict";
  const TRACES = {data_json};

  // ── Run history table ────────────────────────────────────────────────
  const tbody = document.getElementById("run-tbody");
  if (TRACES.length === 0) {{
    const span = document.createElement("td");
    span.colSpan = 8;
    span.className = "empty-table";
    span.textContent = "No trace files loaded.";
    const row = document.createElement("tr");
    row.appendChild(span);
    tbody.appendChild(row);
  }} else {{
    // Show newest first in the table
    [...TRACES].reverse().forEach(function (t) {{
      const dt = t.started_at ? new Date(t.started_at) : null;
      const ts = dt ? dt.toLocaleString(undefined, {{
        year: "numeric", month: "short", day: "2-digit",
        hour: "2-digit", minute: "2-digit", second: "2-digit"
      }}) : "—";
      const dur = t.duration_s != null ? t.duration_s.toFixed(2) + "s" : "—";
      const shortId = (t.run_id || "").substring(0, 8);

      let badge;
      if (t.critical === 0) {{
        badge = '<span class="badge badge-pass">PASS</span>';
      }} else {{
        badge = '<span class="badge badge-fail">FAIL</span>';
      }}

      const row = document.createElement("tr");
      row.innerHTML =
        '<td>' + ts + '</td>' +
        '<td><span class="run-id-short">' + shortId + '</span></td>' +
        '<td>' + dur + '</td>' +
        '<td>' + t.files_scanned + '</td>' +
        '<td class="sev-c">' + t.critical + '</td>' +
        '<td class="sev-w">' + t.warning + '</td>' +
        '<td class="sev-i">' + t.info + '</td>' +
        '<td>' + badge + '</td>';
      tbody.appendChild(row);
    }});
  }}

  // ── Trend chart (inline SVG) ─────────────────────────────────────────
  const svg = document.getElementById("trend-svg");
  const W = svg.parentElement.clientWidth - 48 || 800;
  const H = 160;
  const PAD = {{ top: 10, right: 20, bottom: 28, left: 40 }};
  svg.setAttribute("viewBox", "0 0 " + W + " " + H);

  if (TRACES.length < 1) {{
    const txt = document.createElementNS("http://www.w3.org/2000/svg", "text");
    txt.setAttribute("x", W / 2); txt.setAttribute("y", H / 2);
    txt.setAttribute("text-anchor", "middle");
    txt.setAttribute("fill", "#7a7f99"); txt.setAttribute("font-size", "13");
    txt.textContent = "No trace data";
    svg.appendChild(txt);
  }} else {{
    const maxVal = Math.max(1, ...TRACES.map(function(t) {{
      return t.critical + t.warning + t.info;
    }}));
    const innerW = W - PAD.left - PAD.right;
    const innerH = H - PAD.top - PAD.bottom;
    const step = TRACES.length > 1 ? innerW / (TRACES.length - 1) : innerW / 2;

    function xPos(i) {{
      return PAD.left + (TRACES.length > 1 ? i * step : innerW / 2);
    }}
    function yPos(v) {{
      return PAD.top + innerH - (v / maxVal) * innerH;
    }}

    // Gridlines
    [0, 0.25, 0.5, 0.75, 1].forEach(function(frac) {{
      const y = PAD.top + innerH * (1 - frac);
      const val = Math.round(frac * maxVal);
      const line = document.createElementNS("http://www.w3.org/2000/svg", "line");
      line.setAttribute("x1", PAD.left); line.setAttribute("x2", PAD.left + innerW);
      line.setAttribute("y1", y); line.setAttribute("y2", y);
      line.setAttribute("stroke", "#2a2d3a"); line.setAttribute("stroke-width", "1");
      svg.appendChild(line);
      const lbl = document.createElementNS("http://www.w3.org/2000/svg", "text");
      lbl.setAttribute("x", PAD.left - 6); lbl.setAttribute("y", y + 4);
      lbl.setAttribute("text-anchor", "end"); lbl.setAttribute("fill", "#7a7f99");
      lbl.setAttribute("font-size", "10");
      lbl.textContent = val;
      svg.appendChild(lbl);
    }});

    // X-axis date labels (up to 6)
    const labelStep = Math.max(1, Math.ceil(TRACES.length / 6));
    TRACES.forEach(function(t, i) {{
      if (i % labelStep !== 0 && i !== TRACES.length - 1) return;
      const dt = t.started_at ? new Date(t.started_at) : null;
      if (!dt) return;
      const lbl = document.createElementNS("http://www.w3.org/2000/svg", "text");
      lbl.setAttribute("x", xPos(i)); lbl.setAttribute("y", H - 4);
      lbl.setAttribute("text-anchor", "middle"); lbl.setAttribute("fill", "#7a7f99");
      lbl.setAttribute("font-size", "9");
      lbl.textContent = dt.toLocaleDateString(undefined, {{month: "short", day: "numeric"}});
      svg.appendChild(lbl);
    }});

    // Draw series: total (area fill), critical, warning, info lines
    function makeLine(getter, color, dash) {{
      if (TRACES.length === 1) {{
        const circle = document.createElementNS("http://www.w3.org/2000/svg", "circle");
        circle.setAttribute("cx", xPos(0)); circle.setAttribute("cy", yPos(getter(TRACES[0])));
        circle.setAttribute("r", "4"); circle.setAttribute("fill", color);
        svg.appendChild(circle);
        return;
      }}
      const pts = TRACES.map(function(t, i) {{
        return xPos(i) + "," + yPos(getter(t));
      }}).join(" ");
      const pl = document.createElementNS("http://www.w3.org/2000/svg", "polyline");
      pl.setAttribute("points", pts);
      pl.setAttribute("fill", "none"); pl.setAttribute("stroke", color);
      pl.setAttribute("stroke-width", dash ? "1.5" : "2");
      if (dash) pl.setAttribute("stroke-dasharray", dash);
      pl.setAttribute("stroke-linejoin", "round"); pl.setAttribute("stroke-linecap", "round");
      svg.appendChild(pl);

      // Dots
      TRACES.forEach(function(t, i) {{
        const c = document.createElementNS("http://www.w3.org/2000/svg", "circle");
        c.setAttribute("cx", xPos(i)); c.setAttribute("cy", yPos(getter(t)));
        c.setAttribute("r", "3"); c.setAttribute("fill", color);
        svg.appendChild(c);
      }});
    }}

    // Shaded area under total line
    if (TRACES.length > 1) {{
      const areaPoints =
        TRACES.map(function(t, i) {{ return xPos(i) + "," + yPos(t.total); }}).join(" ") +
        " " + xPos(TRACES.length - 1) + "," + (PAD.top + innerH) +
        " " + xPos(0) + "," + (PAD.top + innerH);
      const area = document.createElementNS("http://www.w3.org/2000/svg", "polygon");
      area.setAttribute("points", areaPoints);
      area.setAttribute("fill", "rgba(79,142,247,0.07)");
      svg.appendChild(area);
    }}

    makeLine(function(t) {{ return t.critical; }}, "#f75050", null);
    makeLine(function(t) {{ return t.warning;  }}, "#f7b050", null);
    makeLine(function(t) {{ return t.info;     }}, "#50b8f7", "4 2");
  }}
}})();
</script>
</body>
</html>
"""
