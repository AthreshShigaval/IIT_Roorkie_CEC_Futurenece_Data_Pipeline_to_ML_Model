"""
dags/nammamart_elt_athresh.py
-----------------------------
NammaMart ELT pipeline: Extract -> Load raw -> Transform in the warehouse with SQL.

  RAW      raw_orders, raw_payments, raw_customers, raw_products, raw_stores
           exact copy of every file, all text, append-only (+ _loaded_at, _source_file, _run_id, _source_line)
  STAGING  stg_customers, stg_products, stg_stores, stg_payments   (rebuilt from this run's raw rows)
           stg_orders  (MERGE on order_id; the increment file updates late status changes)
           every row carries dq_status / failure_reason / dq_warnings
  MART     mart_daily_store_kpis, mart_category_margin, mart_delivery_sla,
           mart_payment_reconciliation, mart_payment_mode_summary, mart_top_customers
           (+ mart_fact_orders view, dim_customer_scd2 loyalty-tier history)

  start_run ─► load_raw_* (parallel) ─► stg_customers / stg_products / stg_stores
                                      ─► stg_orders (orders.csv) ─► merge_increments ─┐
                                      ─► stg_payments ────────────────────────────────┼─► dq_checks
                                                                                       ├─► quarantine
                                                                                       └─► marts.* ─► audit_run

Every transformation is a .sql file in include/sql/elt/. Python only moves files and runs SQL.
Warehouse: include/warehouse_athresh.duckdb      Schedule: 09:00 Asia/Kolkata, Monday-Saturday
"""

import os
from datetime import timedelta

import pendulum
from airflow.sdk.exceptions import AirflowFailException
from airflow.sdk import CronTriggerTimetable, Param, dag, get_current_context, task, task_group

from include.nammamart import config as cfg
from include.nammamart.elt_runner import count_rows, load_raw, run_sql_file, source_columns, sql_variables
from include.nammamart.observability import finish_run, notify_failure
from include.nammamart.warehouse import connect, ensure_metadata, ensure_quarantine_table, write_dq_results

DAG_ID = "nammamart_elt_athresh"
ENTITIES = ["orders", "payments", "customers", "products", "stores"]
MASTERS = ["customers", "products", "stores"]
MARTS = ["mart_daily_store_kpis", "mart_category_margin", "mart_delivery_sla", "mart_top_customers"]

default_args = {
    "owner": cfg.OWNER,
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "execution_timeout": timedelta(minutes=10),
    "on_failure_callback": notify_failure,
}


def _vars(run: dict, **extra) -> dict:
    return sql_variables(run["run_id"], DAG_ID, run["run_time_ist"], **extra)


def _preview(con, table: str, limit: int = 5):
    df = con.execute(f"SELECT * FROM {table} LIMIT {limit}").df()
    total = con.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
    print(f"[Preview] {table}: {total} rows\n{df.to_string(index=False)}")
    return total


@dag(
    dag_id=DAG_ID,
    description="NammaMart ELT: land raw text in DuckDB, transform with SQL into staging and marts",
    schedule=CronTriggerTimetable("0 9 * * 1-6", timezone="Asia/Kolkata"),   # 09:00 IST Mon-Sat = 03:30 UTC
    start_date=pendulum.datetime(2026, 10, 1, tz="Asia/Kolkata"),
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=25),
    default_args=default_args,
    params={
        "data_dir": Param(cfg.DEFAULT_DATA_DIR, type="string",
                          description="Folder with the NammaMart CSVs (experiments point this at a copy)"),
        "apply_increments": Param(True, type="boolean",
                                  description="Also load orders_<date>_increment.csv files found in data_dir"),
    },
    tags=["nammamart", "elt", "athresh"],
    doc_md=__doc__,
)
def nammamart_elt_athresh():

    @task
    def start_run():
        context = get_current_context()
        data_dir = cfg.resolve_data_dir(context["params"]["data_dir"])
        run = {
            "run_id": context["run_id"],
            "data_dir": data_dir,
            "increments": cfg.increment_files(data_dir) if context["params"]["apply_increments"] else [],
            "start_time": pendulum.now("UTC").naive().isoformat(),
            "run_time_ist": pendulum.now("Asia/Kolkata").naive().isoformat(sep=" "),
        }
        with connect() as con:
            ensure_metadata(con)
            ensure_quarantine_table(con, "elt_quarantine")
            run_sql_file(con, "00_setup.sql", _vars(run))
        print(f"[Start] {run}")
        return run

    # ── EXTRACT + LOAD RAW (parallel, no cleaning) ───────────────────────────

    def _load(run, file_name, entity):
        path = os.path.join(run["data_dir"], file_name)
        if not os.path.exists(path):
            raise FileNotFoundError(f"Source file missing: {path}")
        with connect() as con:
            cols = source_columns(con, path)
            expected = cfg.SOURCES[entity]["columns"]
            missing = [c for c in expected if c not in cols]
            extra = [c for c in cols if c not in expected]
            ok = not missing and not extra
            rows = count_rows(con, path)
            write_dq_results(con, [{
                "check_name": f"{entity}_schema_columns", "rows_checked": rows, "rows_failed": 0 if ok else rows,
                "source_file": file_name,
                "details": "columns match contract" if ok else f"missing={missing} unexpected={extra}"}],
                run["run_id"], DAG_ID)
            if not ok:  # a changed contract is not transient: fail now, skip the retries
                raise AirflowFailException(f"{file_name}: schema check failed, missing={missing} unexpected={extra}. "
                                 f"Nothing was loaded; fix the export or agree a new contract.")
            result = load_raw(con, path, entity, run["run_id"])
        print(f"[Raw] {file_name} -> raw_{entity}: {result['rows']} rows")
        return result["rows"]

    @task
    def load_raw_file(run: dict, entity: str):
        rows = _load(run, cfg.SOURCES[entity]["file"], entity)
        get_current_context()["ti"].xcom_push(key="rows_extracted", value=rows)
        return rows

    @task
    def load_raw_increments(run: dict):
        rows = sum(_load(run, f, "orders") for f in run["increments"])
        if not run["increments"]:
            print("[Raw] no increment files to load")
        get_current_context()["ti"].xcom_push(key="rows_extracted", value=rows)
        return rows

    # ── TRANSFORM IN THE WAREHOUSE (SQL) ─────────────────────────────────────

    @task
    def run_sql(run: dict, sql_file: str, preview: str = None):
        with connect() as con:
            run_sql_file(con, sql_file, _vars(run))
            if preview:
                _preview(con, preview)

    @task
    def stg_orders(run: dict):
        return _record_loaded([_merge_orders(run, cfg.SOURCES["orders"]["file"])])

    @task
    def merge_increments(run: dict):
        if not run["increments"]:
            print("[Merge] no increment files this run")
        return _record_loaded([_merge_orders(run, f) for f in run["increments"]])

    def _record_loaded(stats: list):
        get_current_context()["ti"].xcom_push(key="rows_loaded", value=sum(s["pass_rows_written"] for s in stats))
        return stats

    def _merge_orders(run, source_file):
        with connect() as con:
            run_sql_file(con, "stg_orders.sql", _vars(run, source_file=source_file))
            log = con.execute("SELECT * FROM elt_merge_log WHERE run_id = ? AND source_file = ?",
                              [run["run_id"], source_file]).df()
            status = con.execute("""SELECT dq_status, count(*) AS rows FROM stg_orders_batch
                                    WHERE _run_id = ? AND _source_file = ? GROUP BY 1""",
                                 [run["run_id"], source_file]).fetchall()
        stats = log.iloc[0].to_dict()
        print(f"[Merge] {source_file}: batch dq_status={dict(status)}")
        print(f"[Merge] stg_orders MERGE: inserted={stats['inserted']} updated={stats['updated']} "
              f"changed={stats['changed']} skipped={stats['skipped']} pass_rows_written={stats['pass_rows_written']}")
        return {k: (int(v) if isinstance(v, (int, float)) else str(v)) for k, v in stats.items()}

    @task
    def dq_checks(run: dict):
        with connect() as con:
            run_sql_file(con, "dq_row_results.sql", _vars(run))
            run_sql_file(con, "dq_table_checks.sql", _vars(run))
            flagged = con.execute("""
                SELECT source_file, check_name, severity, rows_failed, details FROM dq_results
                WHERE run_id = ? AND dag_id = ? AND rows_failed > 0 ORDER BY source_file, check_name
            """, [run["run_id"], DAG_ID]).df()
        print(f"[DQ] checks with failures this run:\n{flagged.to_string(index=False)}")

    @task
    def quarantine(run: dict):
        with connect() as con:
            run_sql_file(con, "elt_quarantine.sql", _vars(run))
            summary = con.execute("""SELECT source_file, count(*) AS rows FROM elt_quarantine
                                     WHERE run_id = ? GROUP BY 1 ORDER BY 1""", [run["run_id"]]).fetchall()
        total = sum(n for _, n in summary)
        get_current_context()["ti"].xcom_push(key="rows_quarantined", value=total)
        print(f"[Quarantine] {total} rows in elt_quarantine for this run: {dict(summary)}")
        return dict(summary)

    @task_group(group_id="marts")
    def build_marts(run: dict):
        fact = run_sql.override(task_id="mart_fact_orders")(run, "mart_fact_orders.sql", "mart_fact_orders")
        for mart in MARTS:
            fact >> run_sql.override(task_id=mart)(run, f"{mart}.sql", mart)
        recon = run_sql.override(task_id="mart_payment_reconciliation")(
            run, "mart_payment_reconciliation.sql", "mart_payment_reconciliation")
        fact >> recon >> run_sql.override(task_id="mart_payment_mode_summary")(
            run, "mart_payment_mode_summary.sql", "mart_payment_mode_summary")
        # bonus: SCD Type 2 customer dimension (loyalty-tier history)
        run_sql.override(task_id="dim_customer_scd2")(run, "mart_dim_customer_scd2.sql", "dim_customer_scd2")

    @task(trigger_rule="all_done", retries=0)
    def audit_run():
        return finish_run(get_current_context(),
                          [f"load_raw_{e}" for e in ENTITIES] + ["load_raw_increments"],
                          ["stg_orders", "merge_increments"], ["quarantine"])

    # ── WIRING ───────────────────────────────────────────────────────────────

    run = start_run()
    raw = {e: load_raw_file.override(task_id=f"load_raw_{e}")(run, e) for e in ENTITIES}
    raw_increments = load_raw_increments(run)

    stg = {e: run_sql.override(task_id=f"stg_{e}")(run, f"stg_{e}.sql", f"stg_{e}") for e in MASTERS}
    for e in MASTERS:
        raw[e] >> stg[e]

    orders = stg_orders(run)
    [raw["orders"], *stg.values()] >> orders
    increments = merge_increments(run)
    [orders, raw_increments] >> increments

    payments = run_sql.override(task_id="stg_payments")(run, "stg_payments.sql", "stg_payments")
    [raw["payments"], raw["orders"], raw_increments] >> payments

    checks = dq_checks(run)
    rejects = quarantine(run)
    marts = build_marts(run)
    for t in (checks, rejects, marts):
        [increments, payments] >> t

    [checks, rejects, marts] >> audit_run()


nammamart_elt_athresh()
