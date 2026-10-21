"""
include/nammamart/compare_pipelines.py
--------------------------------------
Proves the ETL and ELT outputs agree (Part B.4). Run after both DAGs have run:

    python include/nammamart/compare_pipelines.py            # from the project root
    astro dev bash -> python include/nammamart/compare_pipelines.py
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

import duckdb  # noqa: E402

from include.nammamart.config import WAREHOUSE_DB  # noqa: E402

CHECKS = {
    "September totals (delivered, after discount)": """
        SELECT '{p}' AS pipeline, count(*) FILTER (WHERE order_status = 'DELIVERED') AS delivered_orders,
               round(sum(net_revenue), 2) AS net_revenue, round(sum(gross_margin), 2) AS gross_margin
        FROM {fact} WHERE order_date < DATE '2026-10-01'""",
    "All orders in the reporting layer": """
        SELECT '{p}' AS pipeline, count(*) AS orders, count(*) FILTER (WHERE order_status = 'RETURNED') AS returned,
               round(sum(net_revenue), 2) AS net_revenue
        FROM {fact}""",
    "Reconciliation": """
        SELECT '{p}' AS pipeline, count(*) AS mismatches, round(sum(unreconciled_amount), 2) AS unreconciled
        FROM {recon}""",
}

ROW_DIFF = """
    WITH e AS (SELECT order_id, order_status, order_amount, net_revenue, round(gross_margin, 4) AS gm, within_15_min
               FROM etl_fact_orders),
         l AS (SELECT order_id, order_status, order_amount, net_revenue, round(gross_margin, 4) AS gm, within_15_min
               FROM mart_fact_orders)
    SELECT 'only in ETL' AS side, * FROM (SELECT * FROM e EXCEPT SELECT * FROM l)
    UNION ALL
    SELECT 'only in ELT', * FROM (SELECT * FROM l EXCEPT SELECT * FROM e)
"""

RECON_DIFF = """
    WITH e AS (SELECT mismatch_reason, order_id, payment_ids, unreconciled_amount FROM etl_payment_reconciliation),
         l AS (SELECT mismatch_reason, order_id, payment_ids, unreconciled_amount FROM mart_payment_reconciliation)
    SELECT 'only in ETL' AS side, * FROM (SELECT * FROM e EXCEPT SELECT * FROM l)
    UNION ALL
    SELECT 'only in ELT', * FROM (SELECT * FROM l EXCEPT SELECT * FROM e)
"""


def main(path=WAREHOUSE_DB):
    con = duckdb.connect(path, read_only=True)
    for title, sql in CHECKS.items():
        print(f"\n== {title}")
        etl = sql.format(p="ETL", fact="etl_fact_orders", recon="etl_payment_reconciliation")
        elt = sql.format(p="ELT", fact="mart_fact_orders", recon="mart_payment_reconciliation")
        print(con.execute(f"{etl} UNION ALL {elt}").df().to_string(index=False))
    for title, sql in [("order rows that differ", ROW_DIFF), ("reconciliation rows that differ", RECON_DIFF)]:
        diff = con.execute(sql).df()
        print(f"\n== {title}: {len(diff)}")
        if len(diff):
            print(diff.head(20).to_string(index=False))
    con.close()


if __name__ == "__main__":
    main(*sys.argv[1:])
