"""
include/nammamart/dq_registry.py
--------------------------------
Single list of every data-quality check. Both pipelines use these names:
  * ETL implements them in pandas  (include/nammamart/etl_checks.py)
  * ELT implements them in SQL     (include/sql/elt/stg_*.sql, dq_*.sql)
and both write one row per check per run to the `dq_results` table.

Severity policy (agreed with finance):
  Critical -> the row is quarantined and never reaches a reporting table.
  Warning  -> the row loads, the problem is recorded in `dq_warnings` and `dq_results`.

Dimension rows (customers, products, stores) are only quarantined for key problems
(missing / duplicate key). Attribute problems in a master are Warnings: quarantining
P0019 for a bad cost_price would silently drop 18 genuine customer orders from revenue.
"""

from collections import namedtuple

Check = namedtuple("Check", "check_name entity dimension severity level description")

CRITICAL_SEVERITY, WARNING_SEVERITY = "Critical", "Warning"
C, W = CRITICAL_SEVERITY, WARNING_SEVERITY
ROW, TABLE = "row", "table"

CHECKS = [
    # ── orders (orders.csv and orders_<date>_increment.csv) ──────────────────
    Check("orders_schema_columns", "orders", "Validity", C, TABLE, "file has exactly the 13 agreed columns"),
    Check("orders_order_id_not_null", "orders", "Completeness", C, ROW, "order_id is present"),
    Check("orders_customer_id_not_null", "orders", "Completeness", C, ROW, "customer_id is present"),
    Check("orders_order_ts_not_null", "orders", "Completeness", C, ROW, "order_ts is present"),
    Check("orders_delivered_has_minutes", "orders", "Completeness", C, ROW,
          "DELIVERED orders have delivery_minutes"),
    Check("orders_volume_vs_previous", "orders", "Completeness", W, TABLE,
          "row count did not drop more than 90% vs the previous run"),
    Check("orders_order_id_unique", "orders", "Uniqueness", C, ROW, "order_id appears once per file"),
    Check("orders_order_ts_format", "orders", "Validity", C, ROW, "order_ts parses as YYYY-MM-DD HH:MM:SS"),
    Check("orders_status_valid", "orders", "Validity", C, ROW, "order_status in DELIVERED/CANCELLED/RETURNED"),
    Check("orders_payment_mode_valid", "orders", "Validity", C, ROW, "payment_mode maps to an allowed mode"),
    Check("orders_numeric_valid", "orders", "Validity", C, ROW,
          "quantity, unit_price, discount_pct, order_amount, delivery_minutes are numeric"),
    Check("orders_discount_range", "orders", "Validity", C, ROW, "discount_pct between 0 and 100"),
    Check("orders_quantity_positive", "orders", "Accuracy", C, ROW, "quantity > 0"),
    Check("orders_amount_formula", "orders", "Accuracy", C, ROW,
          "order_amount = quantity x unit_price x (1 - discount_pct/100)"),
    Check("orders_delivery_under_max", "orders", "Accuracy", C, ROW, "delivery_minutes between 1 and 120"),
    Check("orders_delivery_normal_range", "orders", "Accuracy", W, ROW, "delivery_minutes between 8 and 35"),
    Check("orders_unit_price_within_mrp", "orders", "Accuracy", W, ROW, "unit_price <= product mrp"),
    Check("orders_store_price_outlier", "orders", "Accuracy", W, TABLE,
          "no store's average unit_price is > 3x the median store (distribution drift)"),
    Check("orders_customer_fk", "orders", "Consistency", C, ROW, "customer_id exists in customers.csv"),
    Check("orders_product_fk", "orders", "Consistency", C, ROW, "product_id exists in products.csv"),
    Check("orders_store_fk", "orders", "Consistency", C, ROW, "store_id exists in stores.csv"),
    Check("orders_status_casing", "orders", "Consistency", W, ROW, "order_status already in canonical casing"),
    Check("orders_payment_mode_casing", "orders", "Consistency", W, ROW,
          "payment_mode already in canonical spelling"),
    Check("orders_store_daily_capacity", "orders", "Consistency", W, TABLE,
          "daily orders per store <= capacity_orders_per_day"),
    Check("orders_ts_not_future", "orders", "Timeliness", C, ROW, "order_ts not after the file's business date"),
    Check("orders_fresh_vs_batch", "orders", "Timeliness", W, TABLE,
          "latest order_ts falls on the file's business date"),
    Check("orders_fresh_vs_run", "orders", "Timeliness", W, TABLE, "latest order_ts is < 1 day before the run"),

    # ── payments ─────────────────────────────────────────────────────────────
    Check("payments_schema_columns", "payments", "Validity", C, TABLE, "file has exactly the 7 agreed columns"),
    Check("payments_payment_id_not_null", "payments", "Completeness", C, ROW, "payment_id is present"),
    Check("payments_order_id_not_null", "payments", "Completeness", C, ROW, "order_id is present"),
    Check("payments_gateway_ref_present", "payments", "Completeness", W, ROW,
          "non-cash payments carry a gateway_ref"),
    Check("payments_payment_id_unique", "payments", "Uniqueness", C, ROW, "payment_id appears once"),
    Check("payments_status_valid", "payments", "Validity", C, ROW, "payment_status in SUCCESS/FAILED/REFUNDED"),
    Check("payments_payment_mode_valid", "payments", "Validity", C, ROW, "payment_mode maps to an allowed mode"),
    Check("payments_ts_format", "payments", "Validity", C, ROW, "payment_ts parses as YYYY-MM-DD HH:MM:SS"),
    Check("payments_amount_numeric", "payments", "Validity", C, ROW, "amount_paid is numeric"),
    Check("payments_amount_positive", "payments", "Accuracy", C, ROW, "SUCCESS payments have amount_paid > 0"),
    Check("payments_order_fk", "payments", "Consistency", C, ROW, "order_id exists in the orders files"),
    Check("payments_amount_matches_order", "payments", "Consistency", W, ROW,
          "SUCCESS amount_paid equals the order's order_amount"),
    Check("payments_mode_casing", "payments", "Consistency", W, ROW, "payment_mode already in canonical spelling"),
    Check("payments_ts_not_future", "payments", "Timeliness", C, ROW,
          "payment_ts not after the file's business date"),
    Check("payments_ts_after_order", "payments", "Timeliness", W, ROW, "payment_ts is not before order_ts"),

    # ── customers ────────────────────────────────────────────────────────────
    Check("customers_schema_columns", "customers", "Validity", C, TABLE, "file has exactly the 10 agreed columns"),
    Check("customers_customer_id_not_null", "customers", "Completeness", C, ROW, "customer_id is present"),
    Check("customers_city_present", "customers", "Completeness", W, ROW, "city is present"),
    Check("customers_customer_id_unique", "customers", "Uniqueness", C, ROW, "one row per customer_id"),
    Check("customers_email_unique", "customers", "Uniqueness", W, ROW, "email belongs to one customer_id"),
    Check("customers_email_format", "customers", "Validity", W, ROW, "email looks like name@domain.tld"),
    Check("customers_phone_format", "customers", "Validity", W, ROW, "phone is 10 digits"),
    Check("customers_pincode_format", "customers", "Validity", W, ROW, "pincode is 6 digits"),
    Check("customers_loyalty_tier_valid", "customers", "Validity", W, ROW, "loyalty_tier is a known tier"),
    Check("customers_loyalty_tier_casing", "customers", "Consistency", W, ROW, "loyalty_tier in Title Case"),
    Check("customers_signup_not_future", "customers", "Timeliness", W, ROW, "signup_date not in the future"),

    # ── products ─────────────────────────────────────────────────────────────
    Check("products_schema_columns", "products", "Validity", C, TABLE, "file has exactly the 10 agreed columns"),
    Check("products_product_id_not_null", "products", "Completeness", C, ROW, "product_id is present"),
    Check("products_category_present", "products", "Completeness", W, ROW, "category is present"),
    Check("products_product_id_unique", "products", "Uniqueness", C, ROW,
          "one row per product_id (the '(old)' version is dropped)"),
    Check("products_category_valid", "products", "Validity", W, ROW, "category is one of the 6 known categories"),
    Check("products_mrp_positive", "products", "Accuracy", W, ROW, "mrp > 0"),
    Check("products_cost_within_mrp", "products", "Accuracy", W, ROW, "cost_price <= mrp"),
    Check("products_launch_not_future", "products", "Timeliness", W, ROW, "launch_date not in the future"),

    # ── stores ───────────────────────────────────────────────────────────────
    Check("stores_schema_columns", "stores", "Validity", C, TABLE, "file has exactly the 8 agreed columns"),
    Check("stores_store_id_not_null", "stores", "Completeness", C, ROW, "store_id is present"),
    Check("stores_zone_present", "stores", "Completeness", W, ROW, "zone is present"),
    Check("stores_store_id_unique", "stores", "Uniqueness", C, ROW, "one row per store_id"),
    Check("stores_zone_valid", "stores", "Validity", W, ROW, "zone in North/South/East/West"),
    Check("stores_pincode_format", "stores", "Validity", W, ROW, "pincode is 6 digits"),
    Check("stores_capacity_positive", "stores", "Accuracy", W, ROW, "capacity_orders_per_day > 0"),
    Check("stores_open_not_future", "stores", "Timeliness", W, ROW, "open_date not in the future"),
]

BY_NAME = {c.check_name: c for c in CHECKS}


def checks_for(entity, level=None, severity=None):
    return [c for c in CHECKS if c.entity == entity and (level is None or c.level == level)
            and (severity is None or c.severity == severity)]


def reason(check_name: str) -> str:
    """Text used in failure_reason / dq_warnings, e.g. 'orders_quantity_positive: quantity > 0'."""
    return f"{check_name}: {BY_NAME[check_name].description}"
