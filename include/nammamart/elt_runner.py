"""
include/nammamart/elt_runner.py
-------------------------------
Thin Python glue for the ELT DAG. No data is transformed in Python:
  * load_raw()     copies a CSV into raw_<entity> with DuckDB's own CSV reader,
                   every column as text, plus _loaded_at / _source_file / _run_id / _source_line
  * run_sql_file() executes one file from include/sql/elt/ with the run's variables
"""

import os

import pendulum

from include.nammamart import config as cfg

ELT_SQL_DIR = os.path.join(cfg.SQL_DIR, "elt")
# A value that never occurs in the exports, so empty fields stay '' (exact copy) instead of NULL
NEVER_NULL = "__NAMMAMART_NEVER_NULL__"


def sql_variables(run_id: str, dag_id: str, run_time_ist: str, **extra) -> dict:
    """Run context and business thresholds from config.py, exposed to SQL as getvariable('...')."""
    return {
        "run_id": run_id,
        "dag_id": dag_id,
        "run_time_ist": run_time_ist,
        "now_utc": pendulum.now("UTC").naive().isoformat(sep=" "),
        "base_batch_date": cfg.BASE_BATCH_DATE.isoformat(),
        "payments_coverage_end": cfg.payments_coverage_end(),
        "amount_tolerance": cfg.AMOUNT_TOLERANCE,
        "max_delivery_minutes": cfg.MAX_DELIVERY_MINUTES,
        "normal_delivery_min": cfg.NORMAL_DELIVERY_RANGE[0],
        "normal_delivery_max": cfg.NORMAL_DELIVERY_RANGE[1],
        "volume_drop_threshold": cfg.VOLUME_DROP_THRESHOLD,
        "price_outlier_factor": cfg.PRICE_OUTLIER_FACTOR,
        "freshness_max_lag_days": cfg.FRESHNESS_MAX_LAG_DAYS,
        **extra,
    }


def set_variables(con, variables: dict):
    for name, value in variables.items():
        con.execute(f"SET VARIABLE {name} = ?", [value])


def run_sql_file(con, file_name: str, variables: dict):
    path = os.path.join(ELT_SQL_DIR, file_name)
    with open(path, encoding="utf-8") as f:
        sql = f.read()
    set_variables(con, variables)
    con.execute(sql)
    print(f"[SQL] executed include/sql/elt/{file_name}")


def _csv_scan(path: str) -> str:
    quoted = path.replace("'", "''")
    return (f"read_csv('{quoted}', header = true, all_varchar = true, delim = ',', quote = '\"', "
            f"nullstr = '{NEVER_NULL}', auto_detect = true)")


def source_columns(con, path: str) -> list:
    return [d[0] for d in con.execute(f"SELECT * FROM {_csv_scan(path)} LIMIT 0").description]


def count_rows(con, path: str) -> int:
    return con.execute(f"SELECT count(*) FROM {_csv_scan(path)}").fetchone()[0]


def load_raw(con, path: str, entity: str, run_id: str) -> dict:
    """
    Append the file to raw_<entity> exactly as received. Never deletes or edits raw rows.
    Idempotent within a run: a retry of the same run does not load the file twice.
    """
    file_name = os.path.basename(path)
    already = con.execute(f"SELECT count(*) FROM raw_{entity} WHERE _run_id = ? AND _source_file = ?",
                          [run_id, file_name]).fetchone()[0]
    if already:
        print(f"[Raw] {file_name} already loaded for this run ({already} rows), skipping")
        return {"rows": already, "skipped": True}
    columns = ", ".join(cfg.SOURCES[entity]["columns"])
    loaded_at = pendulum.now("UTC").naive().isoformat(sep=" ")
    con.execute("SET threads = 1")  # keep the scan in file order so _source_line matches the file
    con.execute("BEGIN")
    con.execute(f"""
        INSERT INTO raw_{entity} ({columns}, _loaded_at, _source_file, _run_id, _source_line)
        SELECT {columns}, CAST(? AS TIMESTAMP), ?, ?, row_number() OVER () + 1
        FROM {_csv_scan(path)}
    """, [loaded_at, file_name, run_id])
    con.execute("COMMIT")
    rows = con.execute(f"SELECT count(*) FROM raw_{entity} WHERE _run_id = ? AND _source_file = ?",
                       [run_id, file_name]).fetchone()[0]
    return {"rows": rows, "skipped": False}
