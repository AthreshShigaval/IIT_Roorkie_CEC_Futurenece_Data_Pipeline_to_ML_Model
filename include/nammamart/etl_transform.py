"""
include/nammamart/etl_transform.py
----------------------------------
ETL transform, load, reconcile and report steps (pandas -> DuckDB star schema).

Star schema
  etl_dim_customer  one row per customer_id, email / phone / name masked
  etl_dim_product   one row per product_id
  etl_dim_store     one row per store_id
  etl_fact_orders   one row per order_id, only rows that passed every Critical check
"""

import pandas as pd

from include.nammamart import config as cfg
from include.nammamart.etl_checks import order_reference, std_customers, std_orders, std_payments, std_products, \
    std_stores
from include.nammamart.masking import mask_email, mask_name, mask_phone

DDL = """
CREATE TABLE IF NOT EXISTS etl_dim_customer (
    customer_id VARCHAR PRIMARY KEY, customer_name_masked VARCHAR, email_masked VARCHAR, phone_masked VARCHAR,
    city VARCHAR, area VARCHAR, pincode VARCHAR, signup_date DATE, loyalty_tier VARCHAR, is_active BOOLEAN,
    dq_warnings VARCHAR, _source_line BIGINT, _run_id VARCHAR, _loaded_at TIMESTAMP
);
CREATE TABLE IF NOT EXISTS etl_dim_product (
    product_id VARCHAR PRIMARY KEY, product_name VARCHAR, category VARCHAR, sub_category VARCHAR, brand VARCHAR,
    mrp DOUBLE, cost_price DOUBLE, unit VARCHAR, is_perishable BOOLEAN, launch_date DATE,
    dq_warnings VARCHAR, _source_line BIGINT, _run_id VARCHAR, _loaded_at TIMESTAMP
);
CREATE TABLE IF NOT EXISTS etl_dim_store (
    store_id VARCHAR PRIMARY KEY, store_name VARCHAR, zone VARCHAR, area VARCHAR, pincode VARCHAR, open_date DATE,
    manager_name VARCHAR, capacity_orders_per_day INTEGER,
    dq_warnings VARCHAR, _source_line BIGINT, _run_id VARCHAR, _loaded_at TIMESTAMP
);
CREATE TABLE IF NOT EXISTS etl_fact_orders (
    order_id VARCHAR PRIMARY KEY, order_ts TIMESTAMP, order_date DATE,
    customer_id VARCHAR, product_id VARCHAR, store_id VARCHAR,
    quantity INTEGER, unit_price DOUBLE, discount_pct DOUBLE, order_amount DOUBLE,
    payment_mode VARCHAR, order_status VARCHAR, channel VARCHAR, delivery_minutes INTEGER,
    cost_price DOUBLE, net_revenue DOUBLE, cost_amount DOUBLE, gross_margin DOUBLE, within_15_min BOOLEAN,
    dq_warnings VARCHAR, _batch_date DATE, _source_file VARCHAR, _source_line BIGINT,
    _run_id VARCHAR, _loaded_at TIMESTAMP
);
"""


def _audit_cols(df, run_id, loaded_at):
    return df.assign(_run_id=run_id, _loaded_at=loaded_at)


# ── Transform ────────────────────────────────────────────────────────────────

def build_dim_customer(passed: pd.DataFrame) -> pd.DataFrame:
    c = std_customers(passed)
    c["loyalty_tier"] = c.loyalty_tier.where(c.loyalty_tier.isin(cfg.VALID_LOYALTY_TIERS), cfg.UNKNOWN)
    out = pd.DataFrame({
        "customer_id": c.customer_id,
        "customer_name_masked": c.full_name.map(mask_name),
        "email_masked": c.email.map(mask_email),
        "phone_masked": c.phone.map(mask_phone),
        "city": c.city, "area": c.area, "pincode": c.pincode,
        "signup_date": c.signup_date.dt.date, "loyalty_tier": c.loyalty_tier, "is_active": c.is_active,
        "dq_warnings": passed.dq_warnings, "_source_line": c._source_line,
    })
    return out.drop_duplicates("customer_id")


def build_dim_product(passed: pd.DataFrame) -> pd.DataFrame:
    p = std_products(passed)
    out = pd.DataFrame({
        "product_id": p.product_id, "product_name": p.product_name,
        "category": p.category.fillna(cfg.UNKNOWN), "sub_category": p.sub_category, "brand": p.brand,
        "mrp": p.mrp, "cost_price": p.cost_price, "unit": p.unit, "is_perishable": p.is_perishable,
        "launch_date": p.launch_date.dt.date, "dq_warnings": passed.dq_warnings, "_source_line": p._source_line,
    })
    return out.drop_duplicates("product_id")


def build_dim_store(passed: pd.DataFrame) -> pd.DataFrame:
    s = std_stores(passed)
    out = pd.DataFrame({
        "store_id": s.store_id, "store_name": s.store_name, "zone": s.zone.fillna(cfg.UNKNOWN), "area": s.area,
        "pincode": s.pincode, "open_date": s.open_date.dt.date, "manager_name": s.manager_name,
        "capacity_orders_per_day": s.capacity_orders_per_day.astype("Int64"),
        "dq_warnings": passed.dq_warnings, "_source_line": s._source_line,
    })
    return out.drop_duplicates("store_id")


def build_fact_orders(passed_frames: list, dim_product: pd.DataFrame) -> pd.DataFrame:
    """
    passed_frames: passed rows of orders.csv and every increment file. When the same order
    appears in more than one file, the newest business date wins (late status changes).
    """
    frames = [f for f in passed_frames if len(f)]
    if not frames:
        return pd.DataFrame()
    raw = pd.concat(frames, ignore_index=True)
    o = std_orders(raw)
    o["dq_warnings"] = raw.dq_warnings
    o = o.sort_values(["_batch_date", "_source_line"], ascending=[False, True]).drop_duplicates("order_id")

    cost = o.product_id.map(dim_product.set_index("product_id").cost_price)
    delivered = o.order_status == cfg.REVENUE_STATUS
    net_revenue = o.order_amount.where(delivered, 0.0)
    cost_amount = (o.quantity * cost).where(delivered, 0.0)
    within = (o.delivery_minutes <= cfg.SLA_MINUTES).astype("boolean").where(delivered, pd.NA)
    fact = pd.DataFrame({
        "order_id": o.order_id, "order_ts": o.order_ts, "order_date": o.order_ts.dt.date,
        "customer_id": o.customer_id, "product_id": o.product_id, "store_id": o.store_id,
        "quantity": o.quantity.astype("Int64"), "unit_price": o.unit_price, "discount_pct": o.discount_pct,
        "order_amount": o.order_amount, "payment_mode": o.payment_mode, "order_status": o.order_status,
        "channel": o.channel, "delivery_minutes": o.delivery_minutes.astype("Int64"),
        "cost_price": cost, "net_revenue": net_revenue, "cost_amount": cost_amount,
        "gross_margin": net_revenue - cost_amount, "within_15_min": within,
        "dq_warnings": o.dq_warnings, "_batch_date": o._batch_date.dt.date,
        "_source_file": o._source_file, "_source_line": o._source_line,
    })
    return fact.sort_values("order_id").reset_index(drop=True)


# ── Load (idempotent) ────────────────────────────────────────────────────────

def load_dimension(con, table: str, df: pd.DataFrame, run_id: str, loaded_at) -> int:
    """Masters are full snapshots: replace the table contents atomically."""
    df = _audit_cols(df, run_id, loaded_at)  # noqa: F841  (referenced by name in SQL)
    con.execute("BEGIN")
    con.execute(f"DELETE FROM {table}")
    con.execute(f"INSERT INTO {table} BY NAME SELECT * FROM df")
    con.execute("COMMIT")
    return len(df)


def merge_fact_orders(con, fact: pd.DataFrame, run_id: str, loaded_at) -> dict:
    """
    MERGE on order_id. A row is only replaced by a version from the same or a newer
    business date, so re-running September never undoes October's RETURNED status.
    """
    if fact.empty:
        return {"inserted": 0, "updated": 0, "changed": 0, "unchanged_or_older": 0}
    incoming = _audit_cols(fact, run_id, loaded_at)  # noqa: F841
    # Cast the incoming rows to the fact table's types (values arrive as text via XCom)
    con.execute("CREATE OR REPLACE TEMP TABLE src AS SELECT * FROM etl_fact_orders WHERE false")
    con.execute("INSERT INTO src BY NAME SELECT * FROM incoming")
    stats = con.execute("""
        SELECT count(*) FILTER (WHERE t.order_id IS NULL) AS inserted,
               count(*) FILTER (WHERE t.order_id IS NOT NULL AND s._batch_date >= t._batch_date) AS updated,
               count(*) FILTER (WHERE t.order_id IS NOT NULL AND s._batch_date >= t._batch_date AND (
                   s.order_status IS DISTINCT FROM t.order_status OR s.order_amount IS DISTINCT FROM t.order_amount
                   OR s.order_ts IS DISTINCT FROM t.order_ts OR s.quantity IS DISTINCT FROM t.quantity
                   OR s.delivery_minutes IS DISTINCT FROM t.delivery_minutes)) AS changed,
               count(*) FILTER (WHERE t.order_id IS NOT NULL AND s._batch_date < t._batch_date) AS older
        FROM src s LEFT JOIN etl_fact_orders t USING (order_id)
    """).fetchone()
    # DuckDB's bare UPDATE / UPDATE SET * are positional, so name every column explicitly
    set_list = ", ".join(f"{c} = s.{c}" for c in incoming.columns if c != "order_id")
    con.execute("BEGIN")
    con.execute(f"""
        MERGE INTO etl_fact_orders t
        USING src s ON t.order_id = s.order_id
        WHEN MATCHED AND s._batch_date >= t._batch_date THEN UPDATE SET {set_list}
        WHEN NOT MATCHED THEN INSERT BY NAME
    """)
    con.execute("COMMIT")
    return {"inserted": stats[0], "updated": stats[1], "changed": stats[2], "unchanged_or_older": stats[3]}


# ── Reconcile ────────────────────────────────────────────────────────────────

RECON_COLUMNS = ["mismatch_reason", "order_id", "payment_ids", "order_status", "payment_mode",
                 "order_amount", "amount_paid", "difference", "unreconciled_amount", "order_date", "store_id"]


def reconcile(fact: pd.DataFrame, payments_raw: pd.DataFrame, order_frames: list) -> pd.DataFrame:
    """
    One row per order/payment mismatch.
      fact          current etl_fact_orders (clean orders only)
      payments_raw  payments.csv exactly as received
      order_frames  every orders file read this run (to tell 'unknown order' from 'rejected order')
    """
    p = std_payments(payments_raw)
    dup = p.payment_id.notna() & p.payment_id.duplicated(keep="first")
    known_orders = set(order_reference(order_frames).index) if order_frames else set()
    clean_ids = set(fact.order_id)
    rows = []

    def add(reason, **kw):
        rows.append({"mismatch_reason": reason, **kw})

    for _, r in p[dup].iterrows():
        add("DUPLICATE_PAYMENT_RECORD", order_id=r.order_id, payment_ids=r.payment_id, payment_mode=r.payment_mode,
            amount_paid=r.amount_paid, unreconciled_amount=r.amount_paid)

    uniq = p[~dup]
    for _, r in uniq.iterrows():
        if pd.isna(r.order_id) or r.order_id not in known_orders:
            add("PAYMENT_WITHOUT_ORDER", order_id=r.order_id, payment_ids=r.payment_id, payment_mode=r.payment_mode,
                amount_paid=r.amount_paid, unreconciled_amount=r.amount_paid)
        elif r.order_id not in clean_ids:
            add("PAYMENT_FOR_REJECTED_ORDER", order_id=r.order_id, payment_ids=r.payment_id,
                payment_mode=r.payment_mode, amount_paid=r.amount_paid, unreconciled_amount=r.amount_paid)

    # Order side: only orders the payment export is expected to cover
    covered = fact[fact.order_ts < pd.Timestamp(cfg.payments_coverage_end())]
    pay = uniq.assign(success_amount=uniq.amount_paid.where(uniq.payment_status == "SUCCESS"))
    agg = pay.groupby("order_id").agg(
        n_any=("payment_id", "size"),
        n_success=("payment_status", lambda s: int((s == "SUCCESS").sum())),
        n_refund=("payment_status", lambda s: int((s == "REFUNDED").sum())),
        success_amount=("success_amount", "sum"),
        payment_ids=("payment_id", lambda s: ",".join(sorted(s.dropna()))),
    )
    m = covered.merge(agg, left_on="order_id", right_index=True, how="left")
    m["n_any"] = m.n_any.fillna(0)
    m["n_success"] = m.n_success.fillna(0)
    m["n_refund"] = m.n_refund.fillna(0)
    m["success_amount"] = m.success_amount.fillna(0.0)

    for _, r in m.iterrows():
        base = dict(order_id=r.order_id, payment_ids=r.payment_ids if isinstance(r.payment_ids, str) else None,
                    order_status=r.order_status, payment_mode=r.payment_mode, order_amount=r.order_amount,
                    order_date=r.order_date, store_id=r.store_id)
        diff = r.order_amount - r.success_amount
        if r.order_status == "DELIVERED":
            if r.n_any == 0:
                add("MISSING_PAYMENT", **base, amount_paid=0.0, difference=diff, unreconciled_amount=r.order_amount)
            elif r.n_success == 0:
                add("PAYMENT_NOT_SUCCESSFUL", **base, amount_paid=0.0, difference=diff,
                    unreconciled_amount=r.order_amount)
            elif abs(diff) > cfg.AMOUNT_TOLERANCE:
                add("AMOUNT_MISMATCH", **base, amount_paid=r.success_amount, difference=diff,
                    unreconciled_amount=abs(diff))
        elif r.order_status == "RETURNED" and r.n_success > 0 and r.n_refund == 0:
            add("REFUND_NOT_RECORDED", **base, amount_paid=r.success_amount, difference=diff,
                unreconciled_amount=r.success_amount)
        elif r.order_status == "CANCELLED" and r.n_success > 0:
            add("PAYMENT_ON_CANCELLED_ORDER", **base, amount_paid=r.success_amount, difference=diff,
                unreconciled_amount=r.success_amount)

    out = pd.DataFrame(rows, columns=RECON_COLUMNS)
    for c in ["order_amount", "amount_paid", "difference", "unreconciled_amount"]:
        out[c] = pd.to_numeric(out[c]).round(2)
    return out.sort_values(["mismatch_reason", "order_id", "payment_ids"]).reset_index(drop=True)


# ── Reports ──────────────────────────────────────────────────────────────────

def daily_zone_revenue(fact, dim_store):
    d = fact[fact.order_status == cfg.REVENUE_STATUS].merge(dim_store[["store_id", "store_name", "zone"]],
                                                            on="store_id", how="left")
    out = (d.groupby(["order_date", "zone", "store_id", "store_name"], dropna=False)
           .agg(delivered_orders=("order_id", "size"), net_revenue=("net_revenue", "sum"),
                gross_margin=("gross_margin", "sum")).reset_index())
    out[["net_revenue", "gross_margin"]] = out[["net_revenue", "gross_margin"]].round(2)
    return out.sort_values(["order_date", "zone", "store_id"]).reset_index(drop=True)


def category_margin(fact, dim_product):
    d = fact[fact.order_status == cfg.REVENUE_STATUS].merge(dim_product[["product_id", "category"]],
                                                            on="product_id", how="left")
    out = (d.groupby("category", dropna=False)
           .agg(delivered_orders=("order_id", "size"), units=("quantity", "sum"), net_revenue=("net_revenue", "sum"),
                cost_amount=("cost_amount", "sum"), gross_margin=("gross_margin", "sum")).reset_index())
    out["margin_pct"] = (100 * out.gross_margin / out.net_revenue).round(2)
    out[["net_revenue", "cost_amount", "gross_margin"]] = out[["net_revenue", "cost_amount", "gross_margin"]].round(2)
    return out.sort_values("net_revenue", ascending=False).reset_index(drop=True)
