"""
include/nammamart/make_experiment_data.py
-----------------------------------------
Builds the Part F observability datasets as *copies* under include/data/experiments/,
so the original exports in include/data/nammamart/ are never edited. Each experiment
is triggered with the DAG param data_dir pointing at its folder.

    python include/nammamart/make_experiment_data.py
"""

import csv
import os
import shutil
import sys
from datetime import datetime, timedelta

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from include.nammamart.config import DEFAULT_DATA_DIR, INCLUDE_DIR, SOURCES, TS_FORMAT  # noqa: E402

OUT = os.path.join(INCLUDE_DIR, "data", "experiments")
BASE_FILES = [s["file"] for s in SOURCES.values()]


def read(name):
    with open(os.path.join(DEFAULT_DATA_DIR, name), newline="", encoding="utf-8") as f:
        rows = list(csv.reader(f))
    return rows[0], rows[1:]


def write(folder, name, header, rows):
    # keep the export's own line endings (CRLF) so untouched rows stay byte-identical
    with open(os.path.join(DEFAULT_DATA_DIR, name), "rb") as f:
        terminator = "\r\n" if b"\r\n" in f.read(4096) else "\n"
    with open(os.path.join(folder, name), "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f, lineterminator=terminator)
        w.writerow(header)
        w.writerows(rows)


def make(name, orders=None, skip=()):
    """Copy the five base files into experiments/<name>/, optionally replacing orders.csv."""
    folder = os.path.join(OUT, name)
    os.makedirs(folder, exist_ok=True)
    for f in BASE_FILES:
        target = os.path.join(folder, f)
        if os.path.exists(target):
            os.remove(target)
        if f in skip or (f == "orders.csv" and orders):
            continue
        shutil.copyfile(os.path.join(DEFAULT_DATA_DIR, f), target)
    if orders:
        write(folder, "orders.csv", *orders)
    print(f"{name:28s} -> {sorted(os.listdir(folder))}")


def main():
    header, rows = read("orders.csv")
    col = {c: i for i, c in enumerate(header)}

    # 4  Missing source file: products.csv is absent
    make("exp04_missing_products", skip={"products.csv"})

    # 5  New bad data: five extra rows, one problem each
    template = rows[0]

    def bad(order_id, **changes):
        r = list(template)
        r[col["order_id"]] = order_id
        for k, v in changes.items():
            r[col[k]] = v
        return r

    price = float(template[col["unit_price"]])  # template is a valid DELIVERED order
    extra = [
        bad("ORD900001", customer_id=""),                                           # null customer
        bad("ORD900002", quantity="-2", order_amount=f"{-2 * price:.1f}", discount_pct="0"),  # negative quantity
        bad("ORD900003", store_id="S42"),                                            # unknown store
        bad("ORD900004", order_status="SHIPPED"),                                    # status not allowed
        bad("ORD900005", order_ts="2027-03-15 10:00:00"),                            # future timestamp
    ]
    make("exp05_new_bad_data", orders=(header, rows + extra))

    # 6  Schema change: order_amount renamed to total
    make("exp06_schema_change", orders=([("total" if c == "order_amount" else c) for c in header], rows))

    # 7  Volume anomaly: only 50 orders
    make("exp07_volume_drop", orders=(header, rows[:50]))

    # 8  Distribution drift: unit_price x100 for store S05 (order_amount untouched)
    def scale(r, cols):
        r = list(r)
        if r[col["store_id"]] == "S05":
            for c in cols:
                r[col[c]] = f"{float(r[col[c]]) * 100:.2f}"
        return r

    make("exp08_price_drift", orders=(header, [scale(r, ["unit_price"]) for r in rows]))
    # 8b Same drift, but the upstream system also recomputed order_amount (so the formula still holds)
    make("exp08b_consistent_drift", orders=(header, [scale(r, ["unit_price", "order_amount"]) for r in rows]))

    # 9  Freshness: every parseable order_ts moved 30 days into the past
    def shift(r):
        r = list(r)
        try:
            r[col["order_ts"]] = (datetime.strptime(r[col["order_ts"]], TS_FORMAT) - timedelta(days=30)).strftime(TS_FORMAT)
        except ValueError:
            pass
        return r

    make("exp09_stale_orders", orders=(header, [shift(r) for r in rows]))

    # 11 (bonus) SCD Type 2: three customers change loyalty tier in the next CRM export
    c_header, c_rows = read("customers.csv")
    upgrades = {"C00199": "Gold", "C00182": "Platinum", "C00222": "Gold"}
    tier = c_header.index("loyalty_tier")
    changed = [[*r[:tier], upgrades.get(r[0], r[tier]), *r[tier + 1:]] for r in c_rows]
    make("exp11_tier_change")
    write(os.path.join(OUT, "exp11_tier_change"), "customers.csv", c_header, changed)


if __name__ == "__main__":
    main()
