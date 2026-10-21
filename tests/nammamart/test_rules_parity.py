"""
tests/nammamart/test_rules_parity.py
------------------------------------
The ETL (pandas) and ELT (SQL) pipelines implement the same rules twice. These tests
pin the two implementations to each other: masking, and the full check run on the
real exports (same rows rejected, for the same reasons).

    pytest tests/nammamart -q          (or: astro dev pytest)
"""

import os

import duckdb
import pandas as pd
import pytest

from include.nammamart import config as cfg
from include.nammamart import etl_checks as ec
from include.nammamart.elt_runner import load_raw, run_sql_file, sql_variables
from include.nammamart.masking import mask_email, mask_name, mask_phone
from include.nammamart.warehouse import ensure_metadata, ensure_quarantine_table

SAMPLES = [
    ("priya.hegde199@example.com", "9774349021", "Priya Hegde"),
    ("arjun.rao307example.com", "996292312", "  Rahul   Kumar  Joshi "),
    (None, "12", "Single"),
    ("ab@x.co", "98-765-43210", ""),
]


@pytest.fixture(scope="module")
def elt(tmp_path_factory):
    """Run the ELT staging SQL on the real exports into a throw-away DuckDB file."""
    con = duckdb.connect(str(tmp_path_factory.mktemp("wh") / "test.duckdb"))
    variables = sql_variables("test_run", "test_dag", "2026-10-21 09:00:00")
    ensure_metadata(con)
    ensure_quarantine_table(con, "elt_quarantine")
    run_sql_file(con, "00_setup.sql", variables)
    for entity, spec in cfg.SOURCES.items():
        load_raw(con, os.path.join(cfg.DEFAULT_DATA_DIR, spec["file"]), entity, "test_run")
    for name in ["stg_customers", "stg_products", "stg_stores"]:
        run_sql_file(con, f"{name}.sql", variables)
    run_sql_file(con, "stg_orders.sql", {**variables, "source_file": "orders.csv"})
    run_sql_file(con, "stg_payments.sql", variables)
    yield con
    con.close()


def test_masking_matches_sql(elt):
    for email, phone, name in SAMPLES:
        sql = elt.execute("SELECT mask_email(?), mask_phone(?), mask_name(?)", [email, phone, name]).fetchone()
        assert sql == (mask_email(email), mask_phone(phone), mask_name(name)), (email, phone, name)


def _python_results():
    d = cfg.DEFAULT_DATA_DIR
    raw = {e: ec.read_source(d, s["file"], e)[0] for e, s in cfg.SOURCES.items()}
    customers, products, stores = (ec.check_customers(raw["customers"]), ec.check_products(raw["products"]),
                                   ec.check_stores(raw["stores"]))
    orders = ec.check_orders(raw["orders"], customers[0], products[0], stores[0], pd.Timestamp("2026-10-21 09:00"))
    payments = ec.check_payments(raw["payments"], [raw["orders"]])
    return {"customers": customers, "products": products, "stores": stores, "orders": orders,
            "payments": payments}


@pytest.mark.parametrize("entity,table", [("customers", "stg_customers"), ("products", "stg_products"),
                                          ("stores", "stg_stores"), ("orders", "stg_orders_batch"),
                                          ("payments", "stg_payments")])
def test_same_rows_rejected_for_same_reasons(elt, entity, table):
    _, failed, _ = _python_results()[entity]
    py = sorted(zip(failed["_source_line"].astype(int), failed["failure_reason"]))
    sql = sorted(elt.execute(f"SELECT _source_line, failure_reason FROM {table} WHERE dq_status = 'FAIL'").fetchall())
    assert py == sql


def test_row_level_counts_match(elt):
    run_sql_file(elt, "dq_row_results.sql", sql_variables("test_run", "test_dag", "2026-10-21 09:00:00"))
    sql = dict(((c, f), n) for c, f, n in elt.execute(
        "SELECT check_name, source_file, rows_failed FROM dq_results WHERE run_id = 'test_run'").fetchall())
    for _, _, results in _python_results().values():
        for r in results:
            if (r["check_name"], r["source_file"]) in sql:
                assert sql[(r["check_name"], r["source_file"])] == r["rows_failed"], r["check_name"]
