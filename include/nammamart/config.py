"""
include/nammamart/config.py
---------------------------
Paths, source-file contracts and business rules shared by the ETL and ELT DAGs.
"""

import glob
import os
import re
from datetime import date, timedelta

OWNER = "athresh"

# /usr/local/airflow inside the Astro containers, the project root when run locally
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
INCLUDE_DIR = os.path.join(PROJECT_ROOT, "include")
DEFAULT_DATA_DIR = os.path.join(INCLUDE_DIR, "data", "nammamart")
WAREHOUSE_DB = os.path.join(INCLUDE_DIR, f"warehouse_{OWNER}.duckdb")
QUARANTINE_DIR = os.path.join(INCLUDE_DIR, "quarantine", OWNER)
SQL_DIR = os.path.join(INCLUDE_DIR, "sql")

# ── Source-file contracts ────────────────────────────────────────────────────
SOURCES = {
    "orders": {
        "file": "orders.csv",
        "key": "order_id",
        "columns": ["order_id", "order_ts", "customer_id", "store_id", "product_id", "quantity", "unit_price",
                    "discount_pct", "order_amount", "payment_mode", "order_status", "channel", "delivery_minutes"],
    },
    "payments": {
        "file": "payments.csv",
        "key": "payment_id",
        "columns": ["payment_id", "order_id", "payment_ts", "payment_mode", "amount_paid", "payment_status",
                    "gateway_ref"],
    },
    "customers": {
        "file": "customers.csv",
        "key": "customer_id",
        "columns": ["customer_id", "full_name", "email", "phone", "city", "area", "pincode", "signup_date",
                    "loyalty_tier", "is_active"],
    },
    "products": {
        "file": "products.csv",
        "key": "product_id",
        "columns": ["product_id", "product_name", "category", "sub_category", "brand", "mrp", "cost_price", "unit",
                    "is_perishable", "launch_date"],
    },
    "stores": {
        "file": "stores.csv",
        "key": "store_id",
        "columns": ["store_id", "store_name", "zone", "area", "pincode", "open_date", "manager_name",
                    "capacity_orders_per_day"],
    },
}
INCREMENT_GLOB = "orders_*_increment.csv"
INCREMENT_RE = re.compile(r"orders_(\d{4}-\d{2}-\d{2})_increment\.csv$")

# The September exports (orders.csv, payments.csv) cover business dates up to and including this day.
BASE_BATCH_DATE = date(2026, 9, 30)

# ── Business rules agreed with ops & finance ─────────────────────────────────
VALID_ORDER_STATUS = ["DELIVERED", "CANCELLED", "RETURNED"]
REVENUE_STATUS = "DELIVERED"
VALID_PAYMENT_MODES = ["UPI", "Card", "Cash on Delivery", "Wallet"]
# Known spellings seen in the exports, mapped to the canonical value (lower-cased, trimmed key)
PAYMENT_MODE_ALIASES = {
    "upi": "UPI",
    "card": "Card",
    "cash on delivery": "Cash on Delivery",
    "cod": "Cash on Delivery",
    "cash": "Cash on Delivery",
    "cashh": "Cash on Delivery",
    "wallet": "Wallet",
}
VALID_PAYMENT_STATUS = ["SUCCESS", "FAILED", "REFUNDED"]
VALID_LOYALTY_TIERS = ["Bronze", "Silver", "Gold", "Platinum"]
VALID_ZONES = ["North", "South", "East", "West"]
VALID_CATEGORIES = ["Fruits & Vegetables", "Dairy & Bakery", "Staples", "Snacks & Beverages", "Personal Care",
                    "Household"]
NULL_TOKENS = ["", "N/A", "NA", "NULL", "NONE", "NAN", "-"]
TS_FORMAT = "%Y-%m-%d %H:%M:%S"
DATE_FORMAT = "%Y-%m-%d"

MAX_DELIVERY_MINUTES = 120          # above this the value is not believable (ops sign-off)
NORMAL_DELIVERY_RANGE = (8, 35)     # outside this the row loads but is flagged
SLA_MINUTES = 15                    # "within 15 minutes" KPI
AMOUNT_TOLERANCE = 0.01             # one paisa
VOLUME_DROP_THRESHOLD = 0.90        # warn when rows drop by more than 90% vs the previous run
PRICE_OUTLIER_FACTOR = 3.0          # store average unit_price > 3x the median store average
FRESHNESS_MAX_LAG_DAYS = 1
UNKNOWN = "Unknown"


def batch_date_for(file_name: str) -> date:
    """Business date a file covers: orders_<date>_increment.csv -> <date>; base exports -> BASE_BATCH_DATE."""
    m = INCREMENT_RE.search(file_name)
    return date.fromisoformat(m.group(1)) if m else BASE_BATCH_DATE


def batch_cutoff_for(file_name: str) -> str:
    """Timestamps at or after this moment are in the future relative to the export (IST, naive)."""
    return (batch_date_for(file_name) + timedelta(days=1)).strftime(TS_FORMAT)


def payments_coverage_end() -> str:
    """Orders placed before this moment are expected to have a payment in payments.csv."""
    return batch_cutoff_for(SOURCES["payments"]["file"])


def increment_files(data_dir: str) -> list:
    """Increment files present in data_dir, oldest business date first."""
    files = [os.path.basename(p) for p in glob.glob(os.path.join(data_dir, INCREMENT_GLOB))]
    return sorted((f for f in files if INCREMENT_RE.search(f)), key=batch_date_for)


def resolve_data_dir(data_dir: str = None) -> str:
    """Experiments may point the DAG at a copy of the data, but only inside include/data/."""
    path = os.path.realpath(data_dir or DEFAULT_DATA_DIR)
    allowed = os.path.realpath(os.path.join(INCLUDE_DIR, "data"))
    if not path.startswith(allowed):
        raise ValueError(f"data_dir must be inside {allowed}, got {path}")
    if not os.path.isdir(path):
        raise FileNotFoundError(f"data_dir does not exist: {path}")
    return path


def safe_run_id(run_id: str) -> str:
    """Airflow run_ids contain ':' and '+', which are not valid in Windows file names (bind mount)."""
    return re.sub(r"[^A-Za-z0-9_.-]", "_", run_id)
