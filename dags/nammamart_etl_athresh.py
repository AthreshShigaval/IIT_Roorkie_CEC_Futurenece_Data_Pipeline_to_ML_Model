"""
dags/nammamart_etl_athresh.py
-----------------------------
NammaMart ETL pipeline: Extract -> Validate -> Transform -> Load (pandas -> DuckDB).
Only rows that pass every Critical check reach the finance tables.

  start_run
     ├─ extract_customers ── validate_customers ─┐
     ├─ extract_products  ── validate_products  ─┼─ validate_orders ─ validate_payments
     ├─ extract_stores    ── validate_stores    ─┘        │                  │
     ├─ extract_orders ─────────────────────────────────── ┤                  │
     ├─ extract_order_increments ───────────────────────── ┘                  │
     └─ extract_payments ───────────────────────────────────────────────────── ┘
  validate_* ─► quarantine_rejects      (include/quarantine/athresh/<file>_failed_<run_id>.csv)
  validate_* ─► transform ─► load_warehouse ─► reconcile_payments ─► build_reports
  everything ─► audit_run (trigger_rule=all_done -> pipeline_runs)

Warehouse: include/warehouse_athresh.duckdb
Schedule:  09:00 Asia/Kolkata, Monday-Saturday
"""

from datetime import timedelta

import pandas as pd
import pendulum
from airflow.sdk.exceptions import AirflowFailException
from airflow.sdk import CronTriggerTimetable, Param, dag, get_current_context, task

from include.nammamart import config as cfg
from include.nammamart import etl_checks as ec
from include.nammamart import etl_transform as et
from include.nammamart.observability import finish_run, from_payload, notify_failure, to_payload
from include.nammamart.warehouse import (connect, ensure_metadata, ensure_quarantine_table, previous_rows_checked,
                                         write_dq_results)

DAG_ID = "nammamart_etl_athresh"
MASTERS = ["customers", "products", "stores"]
EXTRACT_TASKS = [f"extract_{e}" for e in ["orders", "payments", "customers", "products", "stores"]] + [
    "extract_order_increments"]

default_args = {
    "owner": cfg.OWNER,
    "retries": 2,                                  # transient: file still being copied, DuckDB lock, Docker hiccup
    "retry_delay": timedelta(minutes=2),
    "execution_timeout": timedelta(minutes=10),    # a stuck task must not eat into the 9:30 stand-up
    "on_failure_callback": notify_failure,
}


def _ctx():
    context = get_current_context()
    return context, context["run_id"], context["ti"]


@dag(
    dag_id=DAG_ID,
    description="NammaMart ETL: validate in Python, load only clean rows to a DuckDB star schema",
    schedule=CronTriggerTimetable("0 9 * * 1-6", timezone="Asia/Kolkata"),   # 09:00 IST Mon-Sat = 03:30 UTC
    start_date=pendulum.datetime(2026, 10, 1, tz="Asia/Kolkata"),
    catchup=False,             # yesterday's report is useless after the stand-up; never back-fill a queue of runs
    max_active_runs=1,         # two runs merging into the same fact table at once is never wanted
    dagrun_timeout=timedelta(minutes=25),
    default_args=default_args,
    params={
        "data_dir": Param(cfg.DEFAULT_DATA_DIR, type="string",
                          description="Folder with the NammaMart CSVs (experiments point this at a copy)"),
        "apply_increments": Param(True, type="boolean",
                                  description="Also load orders_<date>_increment.csv files found in data_dir"),
    },
    tags=["nammamart", "etl", "athresh"],
    doc_md=__doc__,
)
def nammamart_etl_athresh():

    @task
    def start_run():
        context, run_id, _ = _ctx()
        data_dir = cfg.resolve_data_dir(context["params"]["data_dir"])
        increments = cfg.increment_files(data_dir) if context["params"]["apply_increments"] else []
        with connect() as con:
            ensure_metadata(con)
            con.execute(et.DDL)
            ensure_quarantine_table(con, "etl_quarantine")
        run = {
            "run_id": run_id,
            "data_dir": data_dir,
            "increments": increments,
            "start_time": pendulum.now("UTC").naive().isoformat(),
            "run_time_ist": pendulum.now("Asia/Kolkata").naive().isoformat(),
        }
        print(f"[Start] {run}")
        return run

    # ── EXTRACT (parallel) ───────────────────────────────────────────────────

    def _read(run, file_name, entity):
        df, schema = ec.read_source(run["data_dir"], file_name, entity)
        with connect() as con:
            write_dq_results(con, [schema], run["run_id"], DAG_ID)
        if schema["rows_failed"]:
            # A changed contract is not transient: fail now (no retries) so the alert goes out at once
            raise AirflowFailException(str(ec.schema_error(schema)))
        print(f"[Extract] {file_name}: {len(df)} rows, columns OK")
        return df

    @task
    def extract_file(run: dict, entity: str):
        _, _, ti = _ctx()
        df = _read(run, cfg.SOURCES[entity]["file"], entity)
        ti.xcom_push(key="rows_extracted", value=len(df))
        return to_payload(df, file=cfg.SOURCES[entity]["file"])

    @task
    def extract_order_increments(run: dict):
        _, _, ti = _ctx()
        payloads = [to_payload(_read(run, f, "orders"), file=f) for f in run["increments"]]
        if not payloads:
            print("[Extract] no increment files to apply")
        ti.xcom_push(key="rows_extracted", value=sum(len(p["rows"]) for p in payloads))
        return payloads

    # ── VALIDATE ─────────────────────────────────────────────────────────────

    def _validated(file_name, passed, failed, results, run):
        with connect() as con:
            write_dq_results(con, results, run["run_id"], DAG_ID)
        flagged = [r for r in results if r["rows_failed"]]
        print(f"[Validate] {file_name}: passed={len(passed)} quarantined={len(failed)}")
        for r in flagged:
            print(f"    {r['check_name']:34s} rows_failed={r['rows_failed']:5d}  {r.get('details') or ''}")
        return {"file": file_name, "passed": to_payload(passed), "failed": to_payload(failed)}

    @task
    def validate_master(extracted: dict, entity: str, run: dict):
        check = {"customers": ec.check_customers, "products": ec.check_products, "stores": ec.check_stores}[entity]
        passed, failed, results = check(from_payload(extracted))
        return _validated(extracted["file"], passed, failed, results, run)

    @task
    def validate_orders(orders: dict, increments: list, customers: dict, products: dict, stores: dict, run: dict):
        masters = [from_payload(m["passed"]) for m in (customers, products, stores)]
        run_time = pd.Timestamp(run["run_time_ist"])
        out = []
        for payload in [orders] + increments:
            with connect() as con:
                previous = previous_rows_checked(con, DAG_ID, run["run_id"], "orders_volume_vs_previous",
                                                 payload["file"])
            passed, failed, results = ec.check_orders(from_payload(payload), *masters, run_time, previous)
            out.append(_validated(payload["file"], passed, failed, results, run))
        return out

    @task
    def validate_payments(payments: dict, orders: dict, increments: list, run: dict):
        order_frames = [from_payload(p) for p in [orders] + increments]
        passed, failed, results = ec.check_payments(from_payload(payments), order_frames)
        return _validated(payments["file"], passed, failed, results, run)

    # ── QUARANTINE ───────────────────────────────────────────────────────────

    @task
    def quarantine_rejects(customers: dict, products: dict, stores: dict, orders: list, payments: dict, run: dict):
        import json
        import os

        _, _, ti = _ctx()
        os.makedirs(cfg.QUARANTINE_DIR, exist_ok=True)
        run_tag = cfg.safe_run_id(run["run_id"])
        frames, summary = [], {}
        for v in [customers, products, stores, *orders, payments]:
            failed = from_payload(v["failed"])
            summary[v["file"]] = len(failed)
            if failed.empty:
                continue
            path = os.path.join(cfg.QUARANTINE_DIR, f"{v['file'].removesuffix('.csv')}_failed_{run_tag}.csv")
            failed.to_csv(path, index=False)
            print(f"[Quarantine] {len(failed):4d} rows -> {path}")
            source_cols = [c for c in failed.columns if not c.startswith("_") and c != "failure_reason"]
            key = cfg.SOURCES.get(v["file"].removesuffix(".csv"), cfg.SOURCES["orders"])["key"]
            frames.append(pd.DataFrame({
                "run_id": run["run_id"], "source_file": v["file"], "source_line": failed["_source_line"],
                "record_key": failed[key], "failure_reason": failed["failure_reason"],
                "raw_record": [json.dumps(r) for r in failed[source_cols].to_dict(orient="records")],
                "quarantined_at": pd.Timestamp.now(tz="UTC").tz_localize(None),
            }))
        q = pd.concat(frames, ignore_index=True) if frames else None  # noqa: F841
        with connect() as con:
            con.execute("BEGIN")
            con.execute("DELETE FROM etl_quarantine WHERE run_id = ?", [run["run_id"]])
            if frames:
                con.execute("INSERT INTO etl_quarantine BY NAME SELECT * FROM q")
            con.execute("COMMIT")
        total = sum(summary.values())
        ti.xcom_push(key="rows_quarantined", value=total)
        print(f"[Quarantine] total {total} rows set aside: {summary}")
        return summary

    # ── TRANSFORM ────────────────────────────────────────────────────────────

    @task
    def transform(customers: dict, products: dict, stores: dict, orders: list):
        dim_customer = et.build_dim_customer(from_payload(customers["passed"]))
        dim_product = et.build_dim_product(from_payload(products["passed"]))
        dim_store = et.build_dim_store(from_payload(stores["passed"]))
        fact = et.build_fact_orders([from_payload(o["passed"]) for o in orders], dim_product)
        print(f"[Transform] dim_customer={len(dim_customer)} dim_product={len(dim_product)} "
              f"dim_store={len(dim_store)} fact_orders={len(fact)}")
        print(f"[Transform] PII check, sample masked customers:\n"
              f"{dim_customer[['customer_id', 'customer_name_masked', 'email_masked', 'phone_masked']].head(3)}")
        return {"dim_customer": to_payload(dim_customer), "dim_product": to_payload(dim_product),
                "dim_store": to_payload(dim_store), "fact": to_payload(fact)}

    # ── LOAD (idempotent) ────────────────────────────────────────────────────

    @task
    def load_warehouse(transformed: dict, run: dict):
        _, _, ti = _ctx()
        loaded_at = pd.Timestamp.now(tz="UTC").tz_localize(None)
        with connect() as con:
            dims = {t: et.load_dimension(con, f"etl_{t}", from_payload(transformed[t]), run["run_id"], loaded_at)
                    for t in ["dim_customer", "dim_product", "dim_store"]}
            stats = et.merge_fact_orders(con, from_payload(transformed["fact"]), run["run_id"], loaded_at)
            total = con.execute("SELECT count(*) FROM etl_fact_orders").fetchone()[0]
        ti.xcom_push(key="rows_loaded", value=stats["inserted"] + stats["updated"])
        print(f"[Load] dimensions replaced: {dims}")
        print(f"[Load] etl_fact_orders MERGE: {stats}  -> table now has {total} rows")
        return {**stats, "fact_rows": total, **dims}

    # ── RECONCILE ────────────────────────────────────────────────────────────

    @task
    def reconcile_payments(load_stats: dict, payments: dict, orders: dict, increments: list, run: dict):
        with connect() as con:
            fact = con.execute("SELECT * FROM etl_fact_orders").df()
            recon = et.reconcile(fact, from_payload(payments), [from_payload(p) for p in [orders] + increments])
            recon["_run_id"] = run["run_id"]
            recon["_reconciled_at"] = pd.Timestamp.now(tz="UTC").tz_localize(None)
            con.execute("CREATE OR REPLACE TABLE etl_payment_reconciliation AS SELECT * FROM recon")
        summary = recon.groupby("mismatch_reason").agg(rows=("mismatch_reason", "size"),
                                                       unreconciled_amount=("unreconciled_amount", "sum"))
        print(f"[Reconcile] {len(recon)} mismatches, unreconciled total "
              f"{recon.unreconciled_amount.sum():,.2f}\n{summary.round(2).to_string()}")
        return {"mismatches": len(recon), "unreconciled_amount": round(float(recon.unreconciled_amount.sum()), 2)}

    # ── REPORT ───────────────────────────────────────────────────────────────

    @task
    def build_reports(_recon: dict):
        with connect() as con:
            fact = con.execute("SELECT * FROM etl_fact_orders").df()
            zone = et.daily_zone_revenue(fact, con.execute("SELECT * FROM etl_dim_store").df())
            margin = et.category_margin(fact, con.execute("SELECT * FROM etl_dim_product").df())
            con.execute("CREATE OR REPLACE TABLE etl_daily_zone_revenue AS SELECT * FROM zone")
            con.execute("CREATE OR REPLACE TABLE etl_category_margin AS SELECT * FROM margin")
        by_zone = zone.groupby("zone").net_revenue.sum().round(2).sort_values(ascending=False)
        print(f"[Report] net revenue by zone (delivered, after discount):\n{by_zone.to_string()}")
        print(f"[Report] latest day by store:\n{zone[zone.order_date == zone.order_date.max()].to_string(index=False)}")
        print(f"[Report] category margin:\n{margin.to_string(index=False)}")
        return {"total_net_revenue": round(float(zone.net_revenue.sum()), 2)}

    # ── RUN AUDIT ────────────────────────────────────────────────────────────

    @task(trigger_rule="all_done", retries=0)
    def audit_run():
        context = get_current_context()
        return finish_run(context, EXTRACT_TASKS, ["load_warehouse"], ["quarantine_rejects"])

    # ── WIRING ───────────────────────────────────────────────────────────────

    run = start_run()
    extracted = {e: extract_file.override(task_id=f"extract_{e}")(run, e)
                 for e in ["orders", "payments", "customers", "products", "stores"]}
    increments = extract_order_increments(run)

    validated = {e: validate_master.override(task_id=f"validate_{e}")(extracted[e], e, run) for e in MASTERS}
    orders_v = validate_orders(extracted["orders"], increments, validated["customers"], validated["products"],
                               validated["stores"], run)
    payments_v = validate_payments(extracted["payments"], extracted["orders"], increments, run)

    quarantined = quarantine_rejects(validated["customers"], validated["products"], validated["stores"], orders_v,
                                     payments_v, run)
    transformed = transform(validated["customers"], validated["products"], validated["stores"], orders_v)
    loaded = load_warehouse(transformed, run)
    recon = reconcile_payments(loaded, extracted["payments"], extracted["orders"], increments, run)
    payments_v >> recon
    reports = build_reports(recon)

    [reports, quarantined] >> audit_run()


nammamart_etl_athresh()
