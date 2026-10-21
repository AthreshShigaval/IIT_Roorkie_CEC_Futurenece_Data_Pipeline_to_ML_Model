"""
tools/rollback_test_runs.py
---------------------------
Removes reporting rows that are still owned by test runs (run_id starting with "exp"),
i.e. rows no production run has superseded. Uses the _run_id lineage column that every
fact / staging row carries. Audit history (dq_results, pipeline_runs, quarantine,
elt_merge_log, raw_*) is kept on purpose. Trigger a normal run afterwards.

Why it exists: experiment 9 shifted every order_ts back 30 days, which made two
genuinely future-dated orders (ORD001089, ORD001693, dated 21 Oct) look valid; they
were merged, and the real file's FAIL version can never overwrite a PASS row.

    python tools/rollback_test_runs.py           # dry run
    python tools/rollback_test_runs.py --apply   # delete (no DAG run may be active)
"""

import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

import duckdb  # noqa: E402

from include.nammamart.config import WAREHOUSE_DB  # noqa: E402

TABLES = ["etl_fact_orders", "stg_orders"]
PATTERN = "exp%"


def main(apply=False):
    con = duckdb.connect(WAREHOUSE_DB, read_only=not apply)
    for table in TABLES:
        rows = con.execute(f"""SELECT _run_id, order_id, order_status, dq_status_or_pass
                               FROM (SELECT *, {'dq_status' if table == 'stg_orders' else "'PASS'"} AS dq_status_or_pass
                                     FROM {table}) WHERE _run_id LIKE ? ORDER BY 1, 2""", [PATTERN]).fetchall()
        print(f"{table}: {len(rows)} rows still owned by test runs")
        for r in rows:
            print("   ", r)
        if apply and rows:
            con.execute(f"DELETE FROM {table} WHERE _run_id LIKE ?", [PATTERN])
            print(f"    deleted {len(rows)} rows from {table}")
    con.close()


if __name__ == "__main__":
    main(apply="--apply" in sys.argv)
