"""
include/nammamart/etl_checks.py
-------------------------------
Python (pandas) implementation of the data-quality rules for the ETL pipeline.
Every check name comes from dq_registry.py; include/sql/elt/ implements the same
rules in SQL, so both pipelines accept and reject exactly the same rows.

Each check_<entity>() returns:
  passed  - raw rows (as received) that passed every Critical check, plus `dq_warnings`
  failed  - raw rows that failed at least one Critical check, plus `failure_reason`
  results - one dict per check for the dq_results table
"""

import os

import pandas as pd

from include.nammamart import config as cfg
from include.nammamart.dq_registry import CRITICAL_SEVERITY, ROW, checks_for, reason

EMAIL_PATTERN = r"[^@\s]+@[^@\s]+\.[^@\s]+"


class SchemaError(Exception):
    """Source file columns differ from the agreed contract. Fail loudly, never load."""


# ── Reading & schema contract ────────────────────────────────────────────────

def read_source(data_dir: str, file_name: str, entity: str):
    """Read a CSV exactly as received (all text, nothing coerced) and check its columns."""
    path = os.path.join(data_dir, file_name)
    if not os.path.exists(path):
        raise FileNotFoundError(f"Source file missing: {path}")
    df = pd.read_csv(path, dtype=str, keep_default_na=False)
    expected = cfg.SOURCES[entity]["columns"]
    missing = [c for c in expected if c not in df.columns]
    extra = [c for c in df.columns if c not in expected]
    ok = not missing and not extra
    result = {
        "check_name": f"{entity}_schema_columns", "rows_checked": len(df), "rows_failed": 0 if ok else len(df),
        "source_file": file_name,
        "details": "columns match contract" if ok else f"missing={missing} unexpected={extra}",
    }
    df["_source_file"] = file_name
    df["_source_line"] = range(2, len(df) + 2)   # line number in the file (header is line 1)
    return df, result


def schema_error(result: dict) -> SchemaError:
    return SchemaError(f"{result['source_file']}: schema check failed ({result['details']})")


# ── Standardisation (shared by validate and transform) ───────────────────────

def clean(s: pd.Series) -> pd.Series:
    """Trim whitespace and turn placeholder tokens ('', 'N/A', 'null', ...) into missing values."""
    s = s.astype("string").str.strip()
    return s.mask(s.str.upper().isin(cfg.NULL_TOKENS))


def num(s):
    return pd.to_numeric(clean(s), errors="coerce").astype("float64")


def ts(s):
    return pd.to_datetime(clean(s), format=cfg.TS_FORMAT, errors="coerce")


def dt(s):
    return pd.to_datetime(clean(s), format=cfg.DATE_FORMAT, errors="coerce")


def map_payment_mode(s):
    return clean(s).str.lower().map(cfg.PAYMENT_MODE_ALIASES)


def std_orders(df):
    o = pd.DataFrame(index=df.index)
    for c in ["order_id", "customer_id", "store_id", "product_id", "channel"]:
        o[c] = clean(df[c])
    o["order_ts_raw"] = clean(df["order_ts"])
    o["order_ts"] = ts(df["order_ts"])
    for c in ["quantity", "unit_price", "discount_pct", "order_amount", "delivery_minutes"]:
        o[c] = num(df[c])
    o["delivery_minutes_raw"] = clean(df["delivery_minutes"])
    o["order_status"] = clean(df["order_status"]).str.upper()
    o["payment_mode"] = map_payment_mode(df["payment_mode"])
    o["_source_file"], o["_source_line"] = df["_source_file"], df["_source_line"]
    o["_batch_date"] = pd.to_datetime(df["_source_file"].map(lambda f: cfg.batch_date_for(f).isoformat()))
    return o


def std_payments(df):
    p = pd.DataFrame(index=df.index)
    for c in ["payment_id", "order_id", "gateway_ref"]:
        p[c] = clean(df[c])
    p["payment_ts_raw"] = clean(df["payment_ts"])
    p["payment_ts"] = ts(df["payment_ts"])
    p["payment_mode"] = map_payment_mode(df["payment_mode"])
    p["amount_paid"] = num(df["amount_paid"])
    p["payment_status"] = clean(df["payment_status"]).str.upper()
    p["_source_line"] = df["_source_line"]
    return p


def std_customers(df):
    c = pd.DataFrame(index=df.index)
    for col in ["customer_id", "full_name", "email", "phone", "city", "area", "pincode"]:
        c[col] = clean(df[col])
    c["signup_date"] = dt(df["signup_date"])
    c["loyalty_tier"] = clean(df["loyalty_tier"]).str.capitalize()
    c["is_active"] = clean(df["is_active"]).str.upper().map({"Y": True, "N": False}).astype("boolean")
    c["_source_line"] = df["_source_line"]
    return c


def std_products(df):
    p = pd.DataFrame(index=df.index)
    for col in ["product_id", "product_name", "category", "sub_category", "brand", "unit"]:
        p[col] = clean(df[col])
    p["mrp"], p["cost_price"] = num(df["mrp"]), num(df["cost_price"])
    p["is_perishable"] = clean(df["is_perishable"]).str.lower().map({"true": True, "false": False}).astype("boolean")
    p["launch_date"] = dt(df["launch_date"])
    p["_source_line"] = df["_source_line"]
    return p


def std_stores(df):
    s = pd.DataFrame(index=df.index)
    for col in ["store_id", "store_name", "zone", "area", "pincode", "manager_name"]:
        s[col] = clean(df[col])
    s["open_date"] = dt(df["open_date"])
    s["capacity_orders_per_day"] = num(df["capacity_orders_per_day"])
    s["_source_line"] = df["_source_line"]
    return s


def duplicated_after_first(key: pd.Series, order_by: pd.DataFrame = None) -> pd.Series:
    """True for every row whose key already appeared earlier (in file order, or in order_by order)."""
    if order_by is None:
        return key.notna() & key.duplicated(keep="first")
    ordered = key.loc[order_by.sort_values(list(order_by.columns)).index]
    return (ordered.notna() & ordered.duplicated(keep="first")).reindex(key.index)


# ── Evaluation helpers ───────────────────────────────────────────────────────

def _join_reasons(flags: dict, names: list, index) -> pd.Series:
    if len(index) == 0 or not names:
        return pd.Series("", index=index, dtype="object")
    frame = pd.DataFrame({n: flags[n].fillna(False).astype(bool) for n in names}, index=index)
    return frame.apply(lambda r: "; ".join(reason(n) for n in names if r[n]), axis=1).astype("object")


def _evaluate(entity: str, raw: pd.DataFrame, flags: dict, source_file: str):
    """Split raw rows into passed / failed and summarise every row-level check."""
    row_checks = checks_for(entity, level=ROW)
    critical = [c.check_name for c in row_checks if c.severity == CRITICAL_SEVERITY]
    warning = [c.check_name for c in row_checks if c.severity != CRITICAL_SEVERITY]
    failure_reason = _join_reasons(flags, critical, raw.index)
    warnings = _join_reasons(flags, warning, raw.index)
    is_failed = failure_reason != ""

    results = [{"check_name": c.check_name, "rows_checked": len(raw),
                "rows_failed": int(flags[c.check_name].fillna(False).astype(bool).sum()),
                "source_file": source_file} for c in row_checks]
    passed = raw[~is_failed].assign(dq_warnings=warnings[~is_failed].replace("", None))
    failed = raw[is_failed].assign(failure_reason=failure_reason[is_failed])
    return passed, failed, results


def table_result(check_name, rows_checked, rows_failed, source_file, details):
    return {"check_name": check_name, "rows_checked": int(rows_checked), "rows_failed": int(rows_failed),
            "source_file": source_file, "details": details}


# ── Master data ──────────────────────────────────────────────────────────────

def check_customers(raw: pd.DataFrame):
    f_name = cfg.SOURCES["customers"]["file"]
    c = std_customers(raw)
    cutoff = pd.Timestamp(cfg.batch_cutoff_for(f_name))
    emails = c["email"].str.lower()
    ids_per_email = c.groupby(emails)["customer_id"].transform("nunique")
    flags = {
        "customers_customer_id_not_null": c.customer_id.isna(),
        "customers_city_present": c.city.isna(),
        "customers_customer_id_unique": duplicated_after_first(c.customer_id),
        "customers_email_unique": emails.notna() & (ids_per_email > 1),
        "customers_email_format": c.email.isna() | ~c.email.str.fullmatch(EMAIL_PATTERN, na=False),
        "customers_phone_format": c.phone.isna() | ~c.phone.str.fullmatch(r"\d{10}", na=False),
        "customers_pincode_format": c.pincode.isna() | ~c.pincode.str.fullmatch(r"\d{6}", na=False),
        "customers_loyalty_tier_valid": ~c.loyalty_tier.isin(cfg.VALID_LOYALTY_TIERS),
        "customers_loyalty_tier_casing": c.loyalty_tier.isin(cfg.VALID_LOYALTY_TIERS)
                                         & (raw.loyalty_tier != c.loyalty_tier),
        "customers_signup_not_future": c.signup_date >= cutoff,
    }
    return _evaluate("customers", raw, flags, f_name)


def check_products(raw: pd.DataFrame):
    f_name = cfg.SOURCES["products"]["file"]
    p = std_products(raw)
    cutoff = pd.Timestamp(cfg.batch_cutoff_for(f_name))
    # When a product_id appears twice, keep the current version, not the one labelled '(old)'
    is_old = p.product_name.str.contains("(old)", case=False, regex=False, na=False)
    order_by = pd.DataFrame({"is_old": is_old, "line": p._source_line})
    flags = {
        "products_product_id_not_null": p.product_id.isna(),
        "products_category_present": p.category.isna(),
        "products_product_id_unique": duplicated_after_first(p.product_id, order_by),
        "products_category_valid": p.category.notna() & ~p.category.isin(cfg.VALID_CATEGORIES),
        "products_mrp_positive": p.mrp.isna() | (p.mrp <= 0),
        "products_cost_within_mrp": p.cost_price > p.mrp,
        "products_launch_not_future": p.launch_date >= cutoff,
    }
    return _evaluate("products", raw, flags, f_name)


def check_stores(raw: pd.DataFrame):
    f_name = cfg.SOURCES["stores"]["file"]
    s = std_stores(raw)
    cutoff = pd.Timestamp(cfg.batch_cutoff_for(f_name))
    flags = {
        "stores_store_id_not_null": s.store_id.isna(),
        "stores_zone_present": s.zone.isna(),
        "stores_store_id_unique": duplicated_after_first(s.store_id),
        "stores_zone_valid": s.zone.notna() & ~s.zone.isin(cfg.VALID_ZONES),
        "stores_pincode_format": s.pincode.isna() | ~s.pincode.str.fullmatch(r"\d{6}", na=False),
        "stores_capacity_positive": s.capacity_orders_per_day.isna() | (s.capacity_orders_per_day <= 0),
        "stores_open_not_future": s.open_date >= cutoff,
    }
    return _evaluate("stores", raw, flags, f_name)


# ── Orders ───────────────────────────────────────────────────────────────────

def check_orders(raw: pd.DataFrame, customers: pd.DataFrame, products: pd.DataFrame, stores: pd.DataFrame,
                 run_time_ist: pd.Timestamp, previous_rows: int = None):
    """
    raw: one orders file (orders.csv or an increment). customers / products / stores are
    the *passed* master rows, used for foreign keys, mrp and store capacity.
    """
    source_file = raw["_source_file"].iloc[0] if len(raw) else "orders.csv"
    o = std_orders(raw)
    cutoff = pd.Timestamp(cfg.batch_cutoff_for(source_file))
    prod = std_products(products).set_index("product_id")
    store = std_stores(stores).set_index("store_id")
    cust_ids = std_customers(customers).customer_id.dropna().tolist()

    expected_amount = o.quantity * o.unit_price * (1 - o.discount_pct / 100)
    mrp = o.product_id.map(prod.mrp)
    dm = o.delivery_minutes
    lo, hi = cfg.NORMAL_DELIVERY_RANGE
    bad_numeric = (o[["quantity", "unit_price", "discount_pct", "order_amount"]].isna().any(axis=1)
                   | (o.delivery_minutes_raw.notna() & dm.isna()))
    flags = {
        "orders_order_id_not_null": o.order_id.isna(),
        "orders_customer_id_not_null": o.customer_id.isna(),
        "orders_order_ts_not_null": o.order_ts_raw.isna(),
        "orders_delivered_has_minutes": (o.order_status == cfg.REVENUE_STATUS) & o.delivery_minutes_raw.isna(),
        "orders_order_id_unique": duplicated_after_first(o.order_id),
        "orders_order_ts_format": o.order_ts_raw.notna() & o.order_ts.isna(),
        "orders_status_valid": ~o.order_status.isin(cfg.VALID_ORDER_STATUS),
        "orders_payment_mode_valid": o.payment_mode.isna(),
        "orders_numeric_valid": bad_numeric,
        "orders_discount_range": (o.discount_pct < 0) | (o.discount_pct > 100),
        "orders_quantity_positive": o.quantity <= 0,
        "orders_amount_formula": (expected_amount - o.order_amount).abs() > cfg.AMOUNT_TOLERANCE,
        "orders_delivery_under_max": (dm <= 0) | (dm > cfg.MAX_DELIVERY_MINUTES),
        "orders_delivery_normal_range": (dm > 0) & (dm <= cfg.MAX_DELIVERY_MINUTES) & ((dm < lo) | (dm > hi)),
        "orders_unit_price_within_mrp": o.unit_price > mrp,
        "orders_customer_fk": o.customer_id.notna() & ~o.customer_id.isin(cust_ids),
        "orders_product_fk": ~o.product_id.isin(prod.index.dropna().tolist()),
        "orders_store_fk": ~o.store_id.isin(store.index.dropna().tolist()),
        "orders_status_casing": o.order_status.isin(cfg.VALID_ORDER_STATUS) & (raw.order_status != o.order_status),
        "orders_payment_mode_casing": o.payment_mode.notna() & (raw.payment_mode != o.payment_mode),
        "orders_ts_not_future": o.order_ts >= cutoff,
    }
    passed, failed, results = _evaluate("orders", raw, flags, source_file)
    results += _orders_table_checks(o, passed.index, source_file, store, run_time_ist, previous_rows, cutoff)
    return passed, failed, results


def _orders_table_checks(o, passed_index, source_file, store, run_time_ist, previous_rows, cutoff):
    n = len(o)
    out = []

    # Volume: more than a 90% drop vs the previous run of the same file
    dropped = previous_rows is not None and n < previous_rows * (1 - cfg.VOLUME_DROP_THRESHOLD)
    out.append(table_result("orders_volume_vs_previous", n, 1 if dropped else 0, source_file,
                            f"rows={n} previous_run_rows={previous_rows}"))

    # Distribution: average unit_price per store vs the median store
    priced = o[o.store_id.notna() & o.unit_price.notna()]
    by_store = priced.groupby("store_id").unit_price.mean()
    median = by_store.median() if len(by_store) else 0
    outliers = sorted(by_store[by_store > cfg.PRICE_OUTLIER_FACTOR * median].index)
    out.append(table_result("orders_store_price_outlier", n, int(priced.store_id.isin(outliers).sum()), source_file,
                            f"median_store_avg_price={median:.2f} outlier_stores={outliers}"))

    # Capacity: passed orders per store per day vs capacity_orders_per_day (stores with capacity <= 0 skipped)
    good = o.loc[passed_index]
    cap = good.store_id.map(store.capacity_orders_per_day)
    daily = (good[cap > 0].assign(order_date=good.order_ts.dt.date).groupby(["store_id", "order_date"])
             .size().rename("orders").reset_index())
    daily["capacity"] = daily.store_id.map(store.capacity_orders_per_day)
    over = daily[daily.orders > daily.capacity]
    out.append(table_result("orders_store_daily_capacity", len(daily), len(over), source_file,
                            f"store_days_over_capacity={len(over)}"))

    # Freshness: latest believable order_ts vs the file's business date and vs run time
    valid_ts = o.order_ts[o.order_ts.notna() & (o.order_ts < cutoff)]
    latest = valid_ts.max() if len(valid_ts) else None
    batch_start = cutoff - pd.Timedelta(days=1)
    stale_batch = latest is None or latest < batch_start
    out.append(table_result("orders_fresh_vs_batch", n, 1 if stale_batch else 0, source_file,
                            f"latest_order_ts={latest} business_date={batch_start.date()}"))
    stale_run = latest is None or latest < run_time_ist - pd.Timedelta(days=cfg.FRESHNESS_MAX_LAG_DAYS)
    lag = None if latest is None else round((run_time_ist - latest).total_seconds() / 86400, 1)
    out.append(table_result("orders_fresh_vs_run", n, 1 if stale_run else 0, source_file,
                            f"latest_order_ts={latest} run_time_ist={run_time_ist} lag_days={lag}"))
    return out


# ── Payments ─────────────────────────────────────────────────────────────────

def order_reference(order_frames: list) -> pd.DataFrame:
    """Latest version of every order seen in this run's order files (newest batch, first line)."""
    o = pd.concat([std_orders(f) for f in order_frames], ignore_index=True)
    o = o[o.order_id.notna()].sort_values(["_batch_date", "_source_line"], ascending=[False, True])
    return o.drop_duplicates("order_id").set_index("order_id")


def check_payments(raw: pd.DataFrame, order_frames: list):
    f_name = cfg.SOURCES["payments"]["file"]
    p = std_payments(raw)
    ref = order_reference(order_frames)
    cutoff = pd.Timestamp(cfg.batch_cutoff_for(f_name))
    order_amount = p.order_id.map(ref.order_amount)
    order_ts = p.order_id.map(ref.order_ts)
    success = p.payment_status == "SUCCESS"
    flags = {
        "payments_payment_id_not_null": p.payment_id.isna(),
        "payments_order_id_not_null": p.order_id.isna(),
        "payments_gateway_ref_present": (p.payment_mode != "Cash on Delivery") & p.gateway_ref.isna(),
        "payments_payment_id_unique": duplicated_after_first(p.payment_id),
        "payments_status_valid": ~p.payment_status.isin(cfg.VALID_PAYMENT_STATUS),
        "payments_payment_mode_valid": p.payment_mode.isna(),
        "payments_ts_format": p.payment_ts_raw.notna() & p.payment_ts.isna(),
        "payments_amount_numeric": p.amount_paid.isna(),
        "payments_amount_positive": success & (p.amount_paid <= 0),
        "payments_order_fk": p.order_id.notna() & ~p.order_id.isin(ref.index.tolist()),
        "payments_amount_matches_order": success & ((p.amount_paid - order_amount).abs() > cfg.AMOUNT_TOLERANCE),
        "payments_mode_casing": p.payment_mode.notna() & (raw.payment_mode != p.payment_mode),
        "payments_ts_not_future": p.payment_ts >= cutoff,
        "payments_ts_after_order": p.payment_ts < order_ts,
    }
    return _evaluate("payments", raw, flags, f_name)
