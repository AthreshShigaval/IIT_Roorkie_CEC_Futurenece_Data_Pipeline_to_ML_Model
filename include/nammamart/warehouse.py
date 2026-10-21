"""
include/nammamart/warehouse.py
------------------------------
DuckDB helpers shared by both DAGs: lock-safe connections, the metadata tables
(dq_check_registry, dq_results, pipeline_runs) and writers for them.
"""

import os
import random
import tempfile
import time
from contextlib import contextmanager

import duckdb
import pandas as pd

from include.nammamart.config import WAREHOUSE_DB
from include.nammamart.dq_registry import CHECKS

METADATA_DDL = """
CREATE TABLE IF NOT EXISTS dq_results (
    run_id VARCHAR, dag_id VARCHAR, check_name VARCHAR, dimension VARCHAR, severity VARCHAR,
    rows_checked BIGINT, rows_failed BIGINT, status VARCHAR, checked_at TIMESTAMP,
    entity VARCHAR, source_file VARCHAR, details VARCHAR
);
CREATE TABLE IF NOT EXISTS pipeline_runs (
    run_id VARCHAR, dag_id VARCHAR, start_time TIMESTAMP, end_time TIMESTAMP,
    rows_extracted BIGINT, rows_loaded BIGINT, rows_quarantined BIGINT, status VARCHAR,
    failed_tasks VARCHAR, dq_critical_failures BIGINT, dq_warnings BIGINT,
    PRIMARY KEY (run_id, dag_id)
);
-- every audit attempt, append-only: a run that failed and was recovered with Clear keeps both rows
CREATE TABLE IF NOT EXISTS pipeline_run_history (
    run_id VARCHAR, dag_id VARCHAR, start_time TIMESTAMP, end_time TIMESTAMP,
    rows_extracted BIGINT, rows_loaded BIGINT, rows_quarantined BIGINT, status VARCHAR,
    failed_tasks VARCHAR, dq_critical_failures BIGINT, dq_warnings BIGINT, audited_at TIMESTAMP
);
"""

QUARANTINE_DDL = """
CREATE TABLE IF NOT EXISTS {table} (
    run_id VARCHAR, source_file VARCHAR, source_line BIGINT, record_key VARCHAR,
    failure_reason VARCHAR, raw_record VARCHAR, quarantined_at TIMESTAMP
);
"""


# Container-local lock file (not on the include/ bind mount, see connect()).
PROCESS_LOCK = os.path.join(tempfile.gettempdir(), "nammamart_warehouse.lock")


@contextmanager
def _process_lock(timeout_s: int = 600):
    """Exclusive flock on a local file; no-op where fcntl is unavailable (Windows host scripts)."""
    try:
        import fcntl
    except ImportError:
        yield
        return
    with open(PROCESS_LOCK, "w") as handle:
        deadline = time.monotonic() + timeout_s
        while True:
            try:
                fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
                break
            except BlockingIOError:
                if time.monotonic() > deadline:
                    raise TimeoutError(f"waited {timeout_s}s for the warehouse lock {PROCESS_LOCK}")
                time.sleep(0.2 + random.random() / 5)
        try:
            yield
        finally:
            fcntl.flock(handle, fcntl.LOCK_UN)


@contextmanager
def connect(path: str = WAREHOUSE_DB, attempts: int = 120):
    """
    DuckDB allows one writing process per file. Parallel Airflow tasks (and the two
    DAGs at 9:00) take turns instead of failing.

    Two layers, because DuckDB's own file lock is not enough here: on the Windows
    bind mount (include/ on 9p/drvfs) the lock hand-over between processes races,
    and 3 of 80 concurrent writes were silently lost in a stress test (0 of 360 with
    this extra container-local flock). Found in observability experiment 7.
    """
    with _process_lock():
        for attempt in range(1, attempts + 1):
            try:
                con = duckdb.connect(path)
                break
            except duckdb.IOException as exc:
                if "lock" not in str(exc).lower() or attempt == attempts:
                    raise
                time.sleep(0.5 + random.random())
        try:
            yield con
        finally:
            con.close()


REGISTRY_DDL = """
CREATE OR REPLACE TABLE dq_check_registry (
    check_order INTEGER, check_name VARCHAR PRIMARY KEY, entity VARCHAR, dimension VARCHAR, severity VARCHAR,
    level VARCHAR, description VARCHAR
);
"""


def ensure_metadata(con):
    """Create the shared metadata tables and (re)publish the check registry from dq_registry.py."""
    con.execute(METADATA_DDL)
    con.execute(REGISTRY_DDL)
    con.executemany("INSERT INTO dq_check_registry VALUES (?, ?, ?, ?, ?, ?, ?)",
                    [(i, *c) for i, c in enumerate(CHECKS, start=1)])


def ensure_quarantine_table(con, table: str):
    con.execute(QUARANTINE_DDL.format(table=table))


def write_dq_results(con, results: list, run_id: str, dag_id: str):
    """
    results: dicts with check_name, rows_checked, rows_failed, source_file and optional details.
    Re-running a task replaces that task's rows, so retries never double-count.
    """
    if not results:
        return
    df = pd.DataFrame(results)
    registry = pd.DataFrame(CHECKS, columns=CHECKS[0]._fields)
    df = df.merge(registry[["check_name", "entity", "dimension", "severity"]], on="check_name", how="left")
    if "details" not in df:
        df["details"] = None
    df["status"] = "PASS"
    df.loc[(df.rows_failed > 0) & (df.severity == "Critical"), "status"] = "FAIL"
    df.loc[(df.rows_failed > 0) & (df.severity == "Warning"), "status"] = "WARN"
    df["run_id"], df["dag_id"] = run_id, dag_id
    df["checked_at"] = pd.Timestamp.now(tz="UTC").tz_localize(None)
    df = df[["run_id", "dag_id", "check_name", "dimension", "severity", "rows_checked", "rows_failed", "status",
             "checked_at", "entity", "source_file", "details"]]
    con.execute("BEGIN")
    con.execute(
        "DELETE FROM dq_results WHERE run_id = ? AND dag_id = ? AND check_name || '|' || source_file IN "
        "(SELECT check_name || '|' || source_file FROM df)", [run_id, dag_id])
    con.execute("INSERT INTO dq_results SELECT * FROM df")
    con.execute("COMMIT")


def previous_rows_checked(con, dag_id: str, run_id: str, check_name: str, source_file: str):
    """rows_checked for this check on the most recent earlier run (used by the volume check)."""
    row = con.execute(
        """SELECT rows_checked FROM dq_results
           WHERE dag_id = ? AND check_name = ? AND source_file = ? AND run_id <> ?
           ORDER BY checked_at DESC LIMIT 1""", [dag_id, check_name, source_file, run_id]).fetchone()
    return row[0] if row else None


def write_pipeline_run(con, record: dict):
    """pipeline_runs keeps the latest state of each run; pipeline_run_history keeps every attempt."""
    cols = ["run_id", "dag_id", "start_time", "end_time", "rows_extracted", "rows_loaded", "rows_quarantined",
            "status", "failed_tasks", "dq_critical_failures", "dq_warnings"]
    values = [record.get(c) for c in cols]
    con.execute("BEGIN")
    con.execute("DELETE FROM pipeline_runs WHERE run_id = ? AND dag_id = ?", [record["run_id"], record["dag_id"]])
    con.execute(f"INSERT INTO pipeline_runs ({', '.join(cols)}) VALUES ({', '.join('?' * len(cols))})", values)
    con.execute(f"INSERT INTO pipeline_run_history ({', '.join(cols)}, audited_at) "
                f"VALUES ({', '.join('?' * len(cols))}, ?)", values + [record["end_time"]])
    con.execute("COMMIT")


def dq_summary(con, run_id: str, dag_id: str):
    """(critical checks failed, warning checks raised) for a run."""
    row = con.execute(
        """SELECT count_if(status = 'FAIL'), count_if(status = 'WARN') FROM dq_results
           WHERE run_id = ? AND dag_id = ?""", [run_id, dag_id]).fetchone()
    return row if row else (0, 0)
