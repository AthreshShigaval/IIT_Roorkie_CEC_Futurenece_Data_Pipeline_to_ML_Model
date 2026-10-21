"""
include/nammamart/observability.py
----------------------------------
Shared by both DAGs:
  * notify_failure  - on_failure_callback; posts to Slack if the Airflow Variable
                      SLACK_WEBHOOK_URL is set, otherwise prints the alert to the task log
  * finish_run      - writes the one-row-per-run record to pipeline_runs
  * to_payload / from_payload - DataFrame <-> XCom (raw text values survive untouched)
"""

import json

import pandas as pd
import pendulum

from include.nammamart.warehouse import METADATA_DDL, connect, dq_summary, write_pipeline_run

FAILED_STATES = {"failed", "upstream_failed"}


def notify_failure(context):
    from airflow.sdk import Variable

    from include.slack_notifier import send_slack_alert

    ti = context["ti"]
    exc = context.get("exception")
    message = (
        f":red_circle: *NammaMart pipeline failure*\n"
        f"DAG: `{ti.dag_id}`  Task: `{ti.task_id}`  Try: {ti.try_number}\n"
        f"Run: `{context['run_id']}`\n"
        f"Error: {type(exc).__name__ if exc else 'unknown'}: {str(exc)[:300] if exc else ''}\n"
        f"Check: Airflow UI > {ti.dag_id} > Grid > {ti.task_id} > Logs, and the pipeline_runs table."
    )
    try:
        webhook = Variable.get("SLACK_WEBHOOK_URL", default=None)
    except Exception:  # Variable backend unavailable must not hide the original failure
        webhook = None
    send_slack_alert(message, webhook_url=webhook)


def to_payload(df: pd.DataFrame, **extra) -> dict:
    return {"columns": list(df.columns), "rows": json.loads(df.to_json(orient="records", date_format="iso")),
            **extra}


def from_payload(payload: dict) -> pd.DataFrame:
    return pd.DataFrame(payload["rows"], columns=payload["columns"])


def _sum_xcom(ti, task_ids, key):
    total = 0
    for task_id in task_ids:
        value = ti.xcom_pull(task_ids=task_id, key=key)
        total += int(value or 0)
    return total


def finish_run(context, extract_tasks, load_tasks, quarantine_tasks):
    """
    Final task (trigger_rule=all_done). Records the run in pipeline_runs, then fails if any
    other task failed, so the DAG run is red in the UI and the failure alert fires.
    """
    from airflow.sdk.exceptions import AirflowFailException

    ti, run_id, dag_id = context["ti"], context["run_id"], context["dag"].dag_id
    states = ti.get_task_states(dag_id=dag_id, run_ids=[run_id]).get(run_id, {})
    failed = sorted(t for t, s in states.items() if s in FAILED_STATES)
    root_causes = sorted(t for t, s in states.items() if s == "failed")    # what to fix
    blocked = len(failed) - len(root_causes)                               # upstream_failed, never ran

    start_xcom = ti.xcom_pull(task_ids="start_run")
    start_time = pd.Timestamp(start_xcom["start_time"]) if start_xcom else None
    end_time = pd.Timestamp(pendulum.now("UTC").naive())
    record = {
        "run_id": run_id, "dag_id": dag_id, "start_time": start_time, "end_time": end_time,
        "rows_extracted": _sum_xcom(ti, extract_tasks, "rows_extracted"),
        "rows_loaded": _sum_xcom(ti, load_tasks, "rows_loaded"),
        "rows_quarantined": _sum_xcom(ti, quarantine_tasks, "rows_quarantined"),
        "status": "FAILED" if failed else "SUCCESS",
        "failed_tasks": (",".join(root_causes) + (f" (+{blocked} upstream_failed)" if blocked else "")) or None,
    }
    with connect() as con:
        con.execute(METADATA_DDL)
        record["dq_critical_failures"], record["dq_warnings"] = dq_summary(con, run_id, dag_id)
        write_pipeline_run(con, record)
    print("[Audit] pipeline_runs row:")
    for k, v in record.items():
        print(f"    {k:22s} {v}")
    if failed:
        raise AirflowFailException(f"Run {run_id} failed in: {record['failed_tasks']}")
    return record["status"]
