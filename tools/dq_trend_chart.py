"""
tools/dq_trend_chart.py
-----------------------
Bonus: data-quality trend built from dq_results. One point per ETL run, in run order:
rows flagged by Critical checks (quarantined) and by Warning checks (loaded, logged).
The same numbers are in the dq_trend table of docs/evidence/evidence_queries.md.

    python tools/dq_trend_chart.py      -> docs/evidence/dq_trend.png
"""

import os
import re
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

import duckdb  # noqa: E402
import matplotlib  # noqa: E402

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

from include.nammamart.config import WAREHOUSE_DB  # noqa: E402

# Reference palette (light mode), validated with the dataviz validator: all checks pass
SURFACE, INK, INK_2, GRID = "#fcfcfb", "#0b0b0b", "#52514e", "#e4e3df"
SERIES = {"Critical rows (quarantined)": "#2a78d6", "Warning rows (loaded, logged)": "#eb6834"}

SQL = """
SELECT p.run_id, p.start_time, p.status,
       coalesce(sum(r.rows_failed) FILTER (WHERE r.severity = 'Critical' AND r.check_name NOT LIKE '%schema%'), 0) AS critical,
       coalesce(sum(r.rows_failed) FILTER (WHERE r.severity = 'Warning'), 0) AS warning
FROM pipeline_runs p LEFT JOIN dq_results r USING (run_id, dag_id)
WHERE p.dag_id = 'nammamart_etl_athresh'
GROUP BY ALL ORDER BY p.start_time
"""


def label(run_id, status):
    name = re.sub(r"__\d{8}T\d{6}$", "", run_id)
    name = re.sub(r"^exp(\d+b?)_", r"\1 ", name).replace("_", " ")
    return name + ("\n(failed)" if status == "FAILED" else "")


def main():
    con = duckdb.connect(WAREHOUSE_DB, read_only=True)
    df = con.execute(SQL).df()
    con.close()
    x = range(len(df))
    labels = [label(r, s) for r, s in zip(df.run_id, df.status)]

    fig, ax = plt.subplots(figsize=(12, 5.2), dpi=150)
    fig.patch.set_facecolor(SURFACE)
    ax.set_facecolor(SURFACE)
    above = df.critical.iloc[-1] >= df.warning.iloc[-1]
    for (name, color), col in zip(SERIES.items(), ["critical", "warning"]):
        ax.plot(x, df[col], color=color, linewidth=2, marker="o", markersize=8,
                markeredgecolor=SURFACE, markeredgewidth=2, label=name, zorder=3)
        # end labels pushed apart so close values never collide
        dy = 8 if (col == "critical") == above else -8
        ax.annotate(name.split(" (")[0], (len(df) - 1, df[col].iloc[-1]), xytext=(12, dy),
                    textcoords="offset points", va="center", color=INK_2, fontsize=9)
    peak = df.critical.idxmax()
    ax.annotate(f"{int(df.critical[peak]):,} critical rows, mostly payments with no\nmatching order (only 50 orders in the file)",
                (peak, df.critical[peak]), xytext=(12, -4), textcoords="offset points", va="top",
                color=INK_2, fontsize=8.5)

    ax.set_xticks(list(x))
    ax.set_xticklabels(labels, rotation=35, ha="right", fontsize=8.5, color=INK_2)
    ax.tick_params(axis="y", colors=INK_2, labelsize=8.5)
    ax.tick_params(length=0)
    ax.grid(axis="y", color=GRID, linewidth=0.8, zorder=0)
    for side in ["top", "right", "left"]:
        ax.spines[side].set_visible(False)
    ax.spines["bottom"].set_color(GRID)
    ax.set_ylabel("rows flagged in the run", color=INK_2, fontsize=9)
    ax.set_ylim(bottom=0)
    ax.set_xlim(-0.5, len(df) - 0.5 + 1.6)
    ax.set_title("NammaMart ETL data-quality trend: rows flagged per run (from dq_results)",
                 loc="left", color=INK, fontsize=12, pad=26)
    ax.legend(loc="upper left", bbox_to_anchor=(0, 1.08), ncol=2, frameon=False, fontsize=9, labelcolor=INK_2)
    fig.tight_layout()
    target = os.path.join(ROOT, "docs", "evidence", "dq_trend.png")
    fig.savefig(target, facecolor=SURFACE)
    print(f"wrote {target}")


if __name__ == "__main__":
    main()
