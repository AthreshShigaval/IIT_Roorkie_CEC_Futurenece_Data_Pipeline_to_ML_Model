-- =============================================================================
-- evidence_queries.sql : audit, lineage, PII and observability evidence used in
-- SUBMISSION.md (Parts D, E, F). Run: python tools/business_questions.py include/sql/evidence_queries.sql
-- =============================================================================

-- name: runs | pipeline_runs: one row per run of each DAG
SELECT dag_id, run_id, status, rows_extracted, rows_loaded, rows_quarantined, dq_critical_failures, dq_warnings,
       failed_tasks, start_time, end_time, round(epoch(end_time - start_time), 1) AS seconds
FROM pipeline_runs ORDER BY start_time, dag_id;

-- name: run_history | pipeline_run_history: every audit attempt (a failed run recovered with Clear keeps both rows)
SELECT dag_id, run_id, status, failed_tasks, rows_loaded, audited_at
FROM pipeline_run_history WHERE run_id IN (SELECT run_id FROM pipeline_run_history GROUP BY 1 HAVING count(*) > 1)
ORDER BY dag_id, audited_at;

-- name: dq_latest | dq_results scorecard, latest ETL run (every check, both severities)
SELECT check_name, dimension, severity, source_file, rows_checked, rows_failed, status
FROM dq_results
WHERE dag_id = 'nammamart_etl_athresh'
  AND run_id = (SELECT run_id FROM pipeline_runs WHERE dag_id = 'nammamart_etl_athresh' ORDER BY end_time DESC LIMIT 1)
ORDER BY CASE status WHEN 'FAIL' THEN 1 WHEN 'WARN' THEN 2 ELSE 3 END, check_name, source_file;

-- name: dq_dimensions | dq_results coverage: checks per quality dimension and severity, latest run of each DAG
SELECT dag_id, dimension, severity, count(*) AS checks, count_if(status <> 'PASS') AS checks_raising,
       sum(rows_failed) AS rows_flagged
FROM dq_results
WHERE (dag_id, run_id) IN (SELECT dag_id, arg_max(run_id, end_time) FROM pipeline_runs GROUP BY dag_id)
GROUP BY ALL ORDER BY dag_id, dimension, severity;

-- name: dq_trend | dq_results over time: failing rows per run (the quality scorecard)
SELECT r.dag_id, r.run_id, sum(r.rows_failed) FILTER (WHERE r.severity = 'Critical') AS critical_rows,
       sum(r.rows_failed) FILTER (WHERE r.severity = 'Warning') AS warning_rows,
       count_if(r.status = 'FAIL') AS failing_checks, count_if(r.status = 'WARN') AS warning_checks
FROM dq_results r GROUP BY ALL ORDER BY min(r.checked_at), r.dag_id;

-- name: recon_sample | Payment reconciliation (ETL), first rows of each mismatch type
SELECT * EXCLUDE (_run_id, _reconciled_at) FROM (
    SELECT *, row_number() OVER (PARTITION BY mismatch_reason ORDER BY order_id) AS rn FROM etl_payment_reconciliation)
WHERE rn <= 2 ORDER BY mismatch_reason, order_id;

-- name: audit_raw_etl | Audit: rejected order ORD002305 exactly as received (ETL quarantine)
SELECT run_id, source_file, source_line, record_key, failure_reason, raw_record
FROM etl_quarantine WHERE record_key = 'ORD002305' ORDER BY quarantined_at DESC LIMIT 1;

-- name: audit_raw_elt | Audit: rejected order ORD002305 exactly as received (ELT raw layer, latest load)
SELECT _run_id, _source_file, _source_line, order_id, order_ts, quantity, unit_price, discount_pct, order_amount,
       order_status
FROM raw_orders WHERE order_id = 'ORD002305' ORDER BY _loaded_at DESC LIMIT 1;

-- name: audit_not_in_revenue | Audit: ORD002305 is not in either CFO revenue table
SELECT 'etl_fact_orders' AS reporting_table, count(*) AS rows_for_ORD002305 FROM etl_fact_orders WHERE order_id = 'ORD002305'
UNION ALL SELECT 'mart_fact_orders', count(*) FROM mart_fact_orders WHERE order_id = 'ORD002305'
UNION ALL SELECT 'stg_orders (dq_status)', count(*) FROM stg_orders WHERE order_id = 'ORD002305' AND dq_status = 'PASS';

-- name: lineage_mart | Lineage: one number on mart_daily_store_kpis (S01 on 2026-09-30)
SELECT order_date, zone, store_id, delivered_orders, net_revenue
FROM mart_daily_store_kpis WHERE store_id = 'S01' AND order_date = DATE '2026-09-30';

-- name: lineage_rows | Lineage: the staging rows behind it, and their exact raw source lines
SELECT f.order_id, f.order_status, f.net_revenue, f._source_file, f._source_line, f._run_id AS merged_by_run,
       r.order_amount AS raw_order_amount, r._loaded_at
FROM mart_fact_orders f
JOIN raw_orders r ON r._run_id = f._run_id AND r._source_file = f._source_file AND r._source_line = f._source_line
WHERE f.store_id = 'S01' AND f.order_date = DATE '2026-09-30' AND f.order_status = 'DELIVERED'
ORDER BY f._source_line;

-- name: lineage_etl | Lineage in ETL: the same number from etl_fact_orders (file + line, but no raw copy in the warehouse)
SELECT order_id, net_revenue, _source_file, _source_line, _run_id
FROM etl_fact_orders WHERE store_id = 'S01' AND order_date = DATE '2026-09-30' AND order_status = 'DELIVERED'
ORDER BY _source_line;

-- name: pii | PII: what a reporting user sees (masked) for the top customer
SELECT customer_id, customer_name_masked, email_masked, phone_masked FROM etl_dim_customer WHERE customer_id = 'C00136'
UNION ALL
SELECT customer_id, customer_name_masked, email_masked, phone_masked FROM stg_customers WHERE customer_id = 'C00136';

-- name: pii_scan | PII scan: no 10-digit phone or full email in any reporting table
SELECT 'etl_dim_customer' AS tbl, count_if(regexp_matches(phone_masked, '^\d{10}$')) AS raw_phones,
       count_if(email_masked NOT LIKE '%***@%') AS unmasked_emails FROM etl_dim_customer
UNION ALL SELECT 'stg_customers', count_if(regexp_matches(phone_masked, '^\d{10}$')),
       count_if(email_masked NOT LIKE '%***@%') FROM stg_customers
UNION ALL SELECT 'mart_top_customers', count_if(regexp_matches(phone_masked, '^\d{10}$')),
       count_if(email_masked NOT LIKE '%***@%') FROM mart_top_customers;

-- name: change_history | Change history: the 10 late returns, ETL fact vs every version production runs received (ELT raw; test runs "exp*" excluded)
SELECT r.order_id, r.order_ts, r._source_file, r.order_status AS status_in_file, r.order_amount,
       f.order_status AS status_now_in_etl_fact, f.net_revenue AS etl_net_revenue_now
FROM (SELECT DISTINCT order_id, order_ts, _source_file, order_status, order_amount FROM raw_orders
      WHERE _run_id NOT LIKE 'exp%' AND order_id IN (SELECT order_id FROM raw_orders WHERE _source_file LIKE 'orders_%_increment.csv'
                         AND order_status = 'RETURNED')) r
LEFT JOIN etl_fact_orders f USING (order_id)
ORDER BY r.order_id, r._source_file DESC;

-- name: september_views | Change history: September revenue as first reported vs restated after the increment
SELECT 'as reported (orders.csv only)' AS view, round(sum(TRY_CAST(order_amount AS DOUBLE)), 2) AS september_net_revenue
FROM (SELECT DISTINCT ON (order_id) order_id, order_amount FROM raw_orders r
      WHERE _source_file = 'orders.csv' AND _run_id NOT LIKE 'exp%'
        AND order_id IN (SELECT order_id FROM stg_orders WHERE dq_status = 'PASS' AND order_date < DATE '2026-10-01')
        AND upper(trim(order_status)) = 'DELIVERED'
      ORDER BY order_id, _loaded_at DESC)
UNION ALL
SELECT 'restated (current status)', round(sum(net_revenue), 2) FROM mart_fact_orders WHERE order_date < DATE '2026-10-01';

-- name: raw_growth | ELT raw layer is append-only: one copy of each file per run
SELECT _source_file, count(DISTINCT _run_id) AS loads, count(*) AS raw_rows, min(_loaded_at) AS first_load,
       max(_loaded_at) AS last_load
FROM raw_orders GROUP BY 1 ORDER BY 1;

-- name: merge_log | ELT MERGE history (stg_orders): inserted / updated / changed per file per run
SELECT run_id, source_file, rows_in_batch, inserted, updated, changed, skipped, pass_rows_written
FROM elt_merge_log ORDER BY merged_at;
