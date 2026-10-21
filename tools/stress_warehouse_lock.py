"""
tools/stress_warehouse_lock.py
------------------------------
Reproduces the lost-write problem found in observability experiment 7 and proves the fix.
Many processes append one row each to a DuckDB file on the include/ bind mount:
  * plain duckdb.connect() + retry on lock  -> some rows silently lost on the Windows mount
  * include.nammamart.warehouse.connect()    -> every row kept (container-local flock)

Run inside the Airflow scheduler container (tools/ is not mounted, so copy it in):
    docker cp tools/stress_warehouse_lock.py <scheduler-container>:/tmp/
    docker exec <scheduler-container> bash -c "cd /usr/local/airflow && ROOT=. python /tmp/stress_warehouse_lock.py"
Result on this project (Windows host, OneDrive folder): plain 72/80 rows kept, warehouse.connect 80/80.
"""

import multiprocessing as mp
import os
import random
import sys
import time

ROOT = os.path.abspath(os.environ.get("ROOT") or os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, ROOT)

import duckdb  # noqa: E402

from include.nammamart.warehouse import connect  # noqa: E402

PATH = os.path.join(ROOT, "include", "stress_lock_test.duckdb")
N = 80


def plain(i):
    for _ in range(200):
        try:
            con = duckdb.connect(PATH)
            break
        except duckdb.IOException:
            time.sleep(0.05 + random.random() / 10)
    con.execute("INSERT INTO t VALUES (?)", [i])
    con.close()


def locked(i):
    with connect(PATH) as con:
        con.execute("INSERT INTO t VALUES (?)", [i])


def trial(fn):
    con = duckdb.connect(PATH)
    con.execute("CREATE OR REPLACE TABLE t(i INT)")
    con.close()
    with mp.Pool(8) as pool:
        pool.map(fn, range(N))
    con = duckdb.connect(PATH)
    kept = con.execute("SELECT count(*) FROM t").fetchone()[0]
    con.close()
    return kept


if __name__ == "__main__":
    for name, fn in [("duckdb.connect only", plain), ("warehouse.connect", locked)]:
        print(f"{name:22s} rows kept: {trial(fn)} of {N}")
    for suffix in ("", ".wal"):
        if os.path.exists(PATH + suffix):
            os.remove(PATH + suffix)
