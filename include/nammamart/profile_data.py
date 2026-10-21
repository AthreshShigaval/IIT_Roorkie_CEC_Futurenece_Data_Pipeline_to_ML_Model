"""
include/nammamart/profile_data.py
---------------------------------
Part 0 — Discovery & data profiling for the NammaMart dataset.

Reads every CSV as raw text (nothing is coerced away) and prints:
  * row counts, nulls per column, duplicate keys
  * min / max of numeric and date columns
  * distinct values of category columns
  * referential-integrity and business-rule violations

Run locally:   python include/nammamart/profile_data.py [data_dir]
Run in Astro:  astro dev bash -> python include/nammamart/profile_data.py
"""

import os
import sys

import pandas as pd

DEFAULT_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "nammamart")

FILES = {
    "orders": ("orders.csv", "order_id"),
    "payments": ("payments.csv", "payment_id"),
    "customers": ("customers.csv", "customer_id"),
    "products": ("products.csv", "product_id"),
    "stores": ("stores.csv", "store_id"),
    "increment": ("orders_2026-10-01_increment.csv", "order_id"),
}
NUMERIC = {
    "orders": ["quantity", "unit_price", "discount_pct", "order_amount", "delivery_minutes"],
    "increment": ["quantity", "unit_price", "discount_pct", "order_amount", "delivery_minutes"],
    "payments": ["amount_paid"],
    "products": ["mrp", "cost_price"],
    "stores": ["capacity_orders_per_day"],
    "customers": [],
}
DATES = {
    "orders": ["order_ts"], "increment": ["order_ts"], "payments": ["payment_ts"],
    "customers": ["signup_date"], "products": ["launch_date"], "stores": ["open_date"],
}
CATEGORIES = {
    "orders": ["order_status", "payment_mode", "channel"],
    "increment": ["order_status", "payment_mode", "channel"],
    "payments": ["payment_mode", "payment_status"],
    "customers": ["city", "loyalty_tier", "is_active"],
    "products": ["category", "unit", "is_perishable"],
    "stores": ["zone"],
}


def section(title):
    print("\n" + "=" * 78 + f"\n{title}\n" + "=" * 78)


def show(label, df, cols=None, n=40):
    print(f"\n-- {label}: {len(df)} row(s)")
    if len(df):
        print((df[cols] if cols else df).head(n).to_string(index=False))


def main(data_dir):
    d = {k: pd.read_csv(os.path.join(data_dir, f), dtype=str, keep_default_na=False)
         for k, (f, _) in FILES.items()}

    for name, df in d.items():
        key = FILES[name][1]
        section(f"{FILES[name][0]}  rows={len(df)}  cols={len(df.columns)}")
        blanks = {c: int((df[c].str.strip() == "").sum()) for c in df.columns}
        print("blank/null per column:", {c: v for c, v in blanks.items() if v})
        dup = df[df[key].duplicated(keep=False)].sort_values(key)
        show(f"duplicate {key}", dup)
        for c in NUMERIC[name]:
            num = pd.to_numeric(df[c], errors="coerce")
            bad = df[(num.isna()) & (df[c].str.strip() != "")]
            print(f"{c:18s} min={num.min()} max={num.max()} non-numeric={len(bad)}")
        for c in DATES[name]:
            ts = pd.to_datetime(df[c], errors="coerce", format="mixed")
            bad = df[(ts.isna()) & (df[c].str.strip() != "")]
            print(f"{c:18s} min={ts.min()} max={ts.max()} unparseable={bad[c].tolist()[:10]}")
        for c in CATEGORIES[name]:
            print(f"{c:18s} {df[c].map(repr).value_counts().to_dict()}")

    o, p, c, pr, s, inc = (d[k] for k in ["orders", "payments", "customers", "products", "stores", "increment"])

    section("Referential integrity")
    for df_name, df in [("orders", o), ("increment", inc)]:
        show(f"{df_name}.customer_id not in customers", df[~df.customer_id.isin(c.customer_id)],
             ["order_id", "customer_id"])
        show(f"{df_name}.product_id not in products", df[~df.product_id.isin(pr.product_id)],
             ["order_id", "product_id"])
        show(f"{df_name}.store_id not in stores", df[~df.store_id.isin(s.store_id)], ["order_id", "store_id"])
    show("payments.order_id not in orders", p[~p.order_id.isin(o.order_id)], ["payment_id", "order_id"])
    show("payments with duplicate order_id", p[p.order_id.duplicated(keep=False)].sort_values("order_id"))
    cancelled_paid = o[(o.order_status.str.upper().str.strip() == "CANCELLED") & o.order_id.isin(p.order_id)]
    show("CANCELLED orders that have a payment", cancelled_paid, ["order_id", "order_status"])
    delivered_unpaid = o[(o.order_status.str.upper().str.strip() == "DELIVERED") & ~o.order_id.isin(p.order_id)]
    show("DELIVERED orders without payment", delivered_unpaid, ["order_id", "order_amount", "payment_mode"])

    section("Business rules")
    for df_name, df in [("orders", o), ("increment", inc)]:
        q = pd.to_numeric(df.quantity, errors="coerce")
        up = pd.to_numeric(df.unit_price, errors="coerce")
        disc = pd.to_numeric(df.discount_pct, errors="coerce")
        amt = pd.to_numeric(df.order_amount, errors="coerce")
        dm = pd.to_numeric(df.delivery_minutes, errors="coerce")
        expected = (q * up * (1 - disc / 100)).round(2)
        show(f"{df_name}: order_amount != formula", df.assign(expected=expected)[(expected - amt).abs() > 0.01],
             ["order_id", "quantity", "unit_price", "discount_pct", "order_amount", "expected"])
        show(f"{df_name}: quantity <= 0", df[q <= 0], ["order_id", "quantity", "order_amount"])
        show(f"{df_name}: discount outside 0-100", df[(disc < 0) | (disc > 100)], ["order_id", "discount_pct"])
        st = df.order_status.str.strip().str.upper()
        show(f"{df_name}: DELIVERED without delivery_minutes", df[(st == "DELIVERED") & dm.isna()],
             ["order_id", "order_status", "delivery_minutes"])
        show(f"{df_name}: delivery_minutes > 120 or <= 0", df[(dm > 120) | (dm <= 0)],
             ["order_id", "order_status", "delivery_minutes"])
        show(f"{df_name}: delivery_minutes outside 8-35 (<=120)", df[((dm < 8) | (dm > 35)) & (dm <= 120) & (dm > 0)],
             ["order_id", "delivery_minutes"])
        ts = pd.to_datetime(df.order_ts, errors="coerce", format="mixed")
        show(f"{df_name}: order_ts outside Sep-Oct 2026", df[(ts > "2026-10-02") | (ts < "2026-09-01")],
             ["order_id", "order_ts"])
        show(f"{df_name}: status not exact allowed", df[~df.order_status.isin(["DELIVERED", "CANCELLED", "RETURNED"])],
             ["order_id", "order_status"])
        show(f"{df_name}: payment_mode not exact allowed",
             df[~df.payment_mode.isin(["UPI", "Card", "Cash on Delivery", "Wallet"])], ["order_id", "payment_mode"])

    # payments vs orders amount
    m = p.merge(o.drop_duplicates("order_id")[["order_id", "order_amount", "order_status", "payment_mode"]],
                on="order_id", how="left", suffixes=("", "_order"))
    diff = (pd.to_numeric(m.amount_paid, errors="coerce") - pd.to_numeric(m.order_amount, errors="coerce")).abs()
    show("SUCCESS payments where amount_paid != order_amount",
         m[(m.payment_status == "SUCCESS") & (diff > 0.01)],
         ["payment_id", "order_id", "amount_paid", "order_amount", "order_status"])
    show("payment_mode differs from order payment_mode",
         m[m.payment_mode.str.strip().str.lower() != m.payment_mode_order.str.strip().str.lower()],
         ["payment_id", "order_id", "payment_mode", "payment_mode_order"])
    show("payments: amount_paid <= 0", m[pd.to_numeric(m.amount_paid, errors="coerce") <= 0],
         ["payment_id", "order_id", "amount_paid", "payment_status"])
    show("payments: blank gateway_ref", p[p.gateway_ref.str.strip() == ""], ["payment_id", "payment_mode", "payment_status"], n=5)
    pts = pd.to_datetime(m.payment_ts, errors="coerce", format="mixed")
    ots = pd.to_datetime(m.order_id.map(o.drop_duplicates("order_id").set_index("order_id").order_ts),
                         errors="coerce", format="mixed")
    show("payment_ts before order_ts", m[pts < ots], ["payment_id", "order_id", "payment_ts"])

    # customers
    show("customers: bad email", c[~c.email.str.match(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")], ["customer_id", "email"])
    show("customers: phone not 10 digits", c[~c.phone.str.match(r"^\d{10}$")], ["customer_id", "phone"])
    show("customers: pincode not 6 digits", c[~c.pincode.str.match(r"^\d{6}$")], ["customer_id", "pincode"])
    show("customers: future signup_date",
         c[pd.to_datetime(c.signup_date, errors="coerce") > "2026-10-01"], ["customer_id", "signup_date"])
    show("customers: duplicate email", c[c.email.str.lower().duplicated(keep=False) & (c.email != "")]
         .sort_values("email"), ["customer_id", "full_name", "email", "phone"])
    show("customers: untrimmed full_name/area", c[(c.full_name != c.full_name.str.strip()) | (c.area != c.area.str.strip())],
         ["customer_id", "full_name", "area"])

    # products
    mrp = pd.to_numeric(pr.mrp, errors="coerce")
    cp = pd.to_numeric(pr.cost_price, errors="coerce")
    show("products: mrp <= 0", pr[mrp <= 0], ["product_id", "mrp", "cost_price"])
    show("products: cost_price > mrp", pr[cp > mrp], ["product_id", "mrp", "cost_price"])
    show("products: blank category", pr[pr.category.str.strip() == ""], ["product_id", "product_name", "category"])
    show("products: duplicate product_name", pr[pr.product_name.duplicated(keep=False)].sort_values("product_name"))
    show("products: future launch_date",
         pr[pd.to_datetime(pr.launch_date, errors="coerce") > "2026-10-01"], ["product_id", "launch_date"])
    # unit_price vs mrp
    j = o.merge(pr.drop_duplicates("product_id")[["product_id", "mrp"]], on="product_id", how="left")
    show("orders: unit_price > mrp", j[pd.to_numeric(j.unit_price, errors="coerce") > pd.to_numeric(j.mrp, errors="coerce")],
         ["order_id", "product_id", "unit_price", "mrp"])

    # stores
    show("stores (full)", s)

    # increment
    section("Increment file")
    existing = inc[inc.order_id.isin(o.order_id)]
    print(f"increment rows: {len(inc)}  existing order_ids: {len(existing)}  new: {len(inc) - len(existing)}")
    cmp = existing.merge(o.drop_duplicates("order_id"), on="order_id", suffixes=("_new", "_old"))
    changed = [col for col in o.columns if col != "order_id" and (cmp[col + "_new"] != cmp[col + "_old"]).any()]
    print("columns changed for existing orders:", changed)
    show("existing orders in increment", cmp, ["order_id", "order_status_old", "order_status_new"])


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else DEFAULT_DIR)
