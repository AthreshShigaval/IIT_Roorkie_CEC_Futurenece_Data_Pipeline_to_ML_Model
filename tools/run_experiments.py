"""
tools/run_experiments.py
------------------------
Drives the Part F observability experiments through the local Airflow REST API
(real scheduler runs, so they show up in the Grid view), then records what
happened: run / task states, retries, durations, XCom load stats and the
warehouse audit tables (pipeline_runs, dq_results, quarantine).

    python tools/run_experiments.py trigger <exp_name> [--etl-only|--elt-only]
    python tools/run_experiments.py clear <exp_name> <task_id>       # experiment 10
    python tools/run_experiments.py report                            # summary of every run

Needs: `astro dev start` running, experiment data from include/nammamart/make_experiment_data.py.
Results are appended to docs/evidence/experiment_results.jsonl.
"""

import json
import os
import sys
import time
import urllib.request

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

API = "http://localhost:8080"
DAGS = {"etl": "nammamart_etl_athresh", "elt": "nammamart_elt_athresh"}
CONTAINER_DATA = "/usr/local/airflow/include/data"
EVIDENCE = os.path.join(ROOT, "docs", "evidence")

EXPERIMENTS = {
    "exp01_happy_path": {"data_dir": f"{CONTAINER_DATA}/nammamart", "apply_increments": False},
    "exp02_rerun": {"data_dir": f"{CONTAINER_DATA}/nammamart", "apply_increments": False},
    "exp03_increment": {"data_dir": f"{CONTAINER_DATA}/nammamart", "apply_increments": True},
    "exp04_missing_products": {"data_dir": f"{CONTAINER_DATA}/experiments/exp04_missing_products"},
    "exp05_new_bad_data": {"data_dir": f"{CONTAINER_DATA}/experiments/exp05_new_bad_data"},
    "exp06_schema_change": {"data_dir": f"{CONTAINER_DATA}/experiments/exp06_schema_change"},
    "exp07_volume_drop": {"data_dir": f"{CONTAINER_DATA}/experiments/exp07_volume_drop"},
    "exp08_price_drift": {"data_dir": f"{CONTAINER_DATA}/experiments/exp08_price_drift"},
    "exp08b_consistent_drift": {"data_dir": f"{CONTAINER_DATA}/experiments/exp08b_consistent_drift"},
    "exp09_stale_orders": {"data_dir": f"{CONTAINER_DATA}/experiments/exp09_stale_orders"},
    "exp11_tier_change": {"data_dir": f"{CONTAINER_DATA}/experiments/exp11_tier_change"},
    "restore": {"data_dir": f"{CONTAINER_DATA}/nammamart", "apply_increments": True},
    "final": {"data_dir": f"{CONTAINER_DATA}/nammamart", "apply_increments": True},
}


def _token():
    with urllib.request.urlopen(urllib.request.Request(f"{API}/auth/token")) as r:
        return json.load(r)["access_token"]


def api(method, path, body=None):
    req = urllib.request.Request(f"{API}/api/v2{path}", method=method,
                                 data=json.dumps(body).encode() if body is not None else None,
                                 headers={"Authorization": f"Bearer {_token()}", "Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req) as r:
            return json.load(r)
    except urllib.error.HTTPError as e:
        raise RuntimeError(f"{method} {path} -> {e.code}: {e.read().decode()[:500]}") from e


def unpause(dag_id):
    api("PATCH", f"/dags/{dag_id}", {"is_paused": False})


def wait(dag_id, run_id, timeout=1200):
    t0 = time.time()
    while time.time() - t0 < timeout:
        state = api("GET", f"/dags/{dag_id}/dagRuns/{run_id}")["state"]
        if state in ("success", "failed"):
            return state
        time.sleep(5)
    return "timeout"


def task_summary(dag_id, run_id):
    tis = api("GET", f"/dags/{dag_id}/dagRuns/{run_id}/taskInstances?limit=100")["task_instances"]
    return sorted(({"task_id": t["task_id"], "state": t["state"], "try_number": t["try_number"],
                    "start": t["start_date"], "end": t["end_date"], "duration_s": t["duration"]} for t in tis),
                  key=lambda t: (t["start"] or "z", t["task_id"]))


def tries(dag_id, run_id, task_id):
    data = api("GET", f"/dags/{dag_id}/dagRuns/{run_id}/taskInstances/{task_id}/tries")
    return [{"try": t["try_number"], "state": t["state"], "start": t["start_date"], "end": t["end_date"]}
            for t in data["task_instances"]]


def xcom(dag_id, run_id, task_id, key="return_value"):
    try:
        return api("GET", f"/dags/{dag_id}/dagRuns/{run_id}/taskInstances/{task_id}/xcomEntries/{key}")["value"]
    except RuntimeError:
        return None


def warehouse_facts(run_id, dag_id):
    import duckdb

    from include.nammamart.config import WAREHOUSE_DB
    con = duckdb.connect(WAREHOUSE_DB, read_only=True)
    q = lambda sql: con.execute(sql, [run_id, dag_id]).df().to_dict(orient="records")  # noqa: E731
    table = "etl_quarantine" if "etl" in dag_id else "elt_quarantine"
    out = {
        "pipeline_run": q("SELECT * FROM pipeline_runs WHERE run_id = ? AND dag_id = ?"),
        "dq_flagged": q("""SELECT source_file, check_name, severity, rows_checked, rows_failed, status, details
                           FROM dq_results WHERE run_id = ? AND dag_id = ? AND rows_failed > 0
                           ORDER BY source_file, check_name"""),
        "quarantine": con.execute(f"""SELECT source_file, count(*) AS rows FROM {table} WHERE run_id = ?
                                      GROUP BY 1 ORDER BY 1""", [run_id]).df().to_dict(orient="records"),
    }
    con.close()
    return out


def record(exp, dag_id, run_id, state):
    result = {"experiment": exp, "dag_id": dag_id, "run_id": run_id, "state": state,
              "tasks": task_summary(dag_id, run_id)}
    if "etl" in dag_id:
        result["load"] = xcom(dag_id, run_id, "load_warehouse")
        result["reconcile"] = xcom(dag_id, run_id, "reconcile_payments")
    else:
        result["merge_orders"] = xcom(dag_id, run_id, "stg_orders")
        result["merge_increments"] = xcom(dag_id, run_id, "merge_increments")
    for t in result["tasks"]:
        if t["try_number"] and t["try_number"] > 1 or t["state"] in ("failed",):
            t["tries"] = tries(dag_id, run_id, t["task_id"])
    result["warehouse"] = warehouse_facts(run_id, dag_id)
    os.makedirs(EVIDENCE, exist_ok=True)
    with open(os.path.join(EVIDENCE, "experiment_results.jsonl"), "a", encoding="utf-8") as f:
        f.write(json.dumps(result, default=str) + "\n")
    return result


def show(result):
    print(f"\n=== {result['experiment']} | {result['dag_id']} | {result['run_id']} -> {result['state']}")
    for t in result["tasks"]:
        extra = f" tries={t['tries']}" if "tries" in t else ""
        print(f"  {t['task_id']:34s} {str(t['state']):16s} try={t['try_number']} {t['duration_s']}s{extra}")
    for k in ("load", "reconcile", "merge_orders", "merge_increments"):
        if k in result:
            print(f"  {k}: {result[k]}")
    w = result["warehouse"]
    print(f"  pipeline_runs: {w['pipeline_run']}")
    print(f"  quarantine: {w['quarantine']}")
    for d in w["dq_flagged"]:
        print(f"  dq  {d['source_file']:32s} {d['check_name']:34s} {d['status']} failed={d['rows_failed']} "
              f"{d['details'] or ''}")


def trigger(exp, which=("etl", "elt")):
    conf = EXPERIMENTS[exp]
    runs = {}
    for key in which:
        dag_id = DAGS[key]
        unpause(dag_id)
        run_id = f"{exp}__{time.strftime('%Y%m%dT%H%M%S')}"
        api("POST", f"/dags/{dag_id}/dagRuns", {"dag_run_id": run_id, "logical_date": None, "conf": conf})
        runs[dag_id] = run_id
        print(f"triggered {dag_id} {run_id} conf={conf}")
    states = {dag_id: wait(dag_id, run_id) for dag_id, run_id in runs.items()}
    for dag_id, run_id in runs.items():
        show(record(exp, dag_id, run_id, states[dag_id]))


def clear(exp, task_id, which=("etl", "elt")):
    """Experiment 10: re-run a failed task and everything downstream of it in the same run."""
    log = os.path.join(EVIDENCE, "experiment_results.jsonl")
    with open(log, encoding="utf-8") as f:
        past = [json.loads(line) for line in f]
    for key in which:
        dag_id = DAGS[key]
        run_id = [r for r in past if r["experiment"] == exp and r["dag_id"] == dag_id][-1]["run_id"]
        tid = task_id if key == "etl" else task_id.replace("extract_", "load_raw_")
        cleared = api("POST", f"/dags/{dag_id}/clearTaskInstances",
                      {"dag_run_id": run_id, "task_ids": [tid], "include_downstream": True,
                       "only_failed": False, "dry_run": False, "reset_dag_runs": True})
        names = sorted(t["task_id"] for t in cleared["task_instances"])
        print(f"cleared {dag_id} {run_id}: {len(names)} tasks -> {names}")
        state = wait(dag_id, run_id)
        show(record("exp10_recovery", dag_id, run_id, state))


if __name__ == "__main__":
    cmd = sys.argv[1]
    which = ("etl",) if "--etl-only" in sys.argv else ("elt",) if "--elt-only" in sys.argv else ("etl", "elt")
    if cmd == "trigger":
        trigger(sys.argv[2], which)
    elif cmd == "clear":
        clear(sys.argv[2], sys.argv[3], which)
