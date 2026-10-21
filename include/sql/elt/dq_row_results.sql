-- =============================================================================
-- dq_row_results.sql : one dq_results row per row-level check per source file,
-- counted from the failed_checks lists written by the stg_*.sql files.
-- =============================================================================
DELETE FROM dq_results
WHERE run_id = getvariable('run_id') AND dag_id = getvariable('dag_id')
  AND check_name IN (SELECT check_name FROM dq_check_registry WHERE level = 'row');

INSERT INTO dq_results (run_id, dag_id, check_name, dimension, severity, rows_checked, rows_failed, status,
                        checked_at, entity, source_file, details)
WITH evaluated AS (
    SELECT 'customers' AS entity, _source_file AS source_file, failed_checks FROM stg_customers
    UNION ALL SELECT 'products', _source_file, failed_checks FROM stg_products
    UNION ALL SELECT 'stores', _source_file, failed_checks FROM stg_stores
    UNION ALL SELECT 'payments', _source_file, failed_checks FROM stg_payments
    UNION ALL SELECT 'orders', _source_file, failed_checks FROM stg_orders_batch WHERE _run_id = getvariable('run_id')
),
files AS (
    SELECT entity, source_file, count(*) AS rows_checked FROM evaluated GROUP BY ALL
),
failures AS (
    SELECT entity, source_file, check_name, count(*) AS rows_failed
    FROM (SELECT entity, source_file, unnest(failed_checks) AS check_name FROM evaluated)
    GROUP BY ALL
)
SELECT
    getvariable('run_id'), getvariable('dag_id'), r.check_name, r.dimension, r.severity,
    f.rows_checked, coalesce(x.rows_failed, 0) AS rows_failed,
    CASE WHEN coalesce(x.rows_failed, 0) = 0 THEN 'PASS' WHEN r.severity = 'Critical' THEN 'FAIL' ELSE 'WARN' END,
    CAST(getvariable('now_utc') AS TIMESTAMP), r.entity, f.source_file, NULL
FROM dq_check_registry r
JOIN files f ON f.entity = r.entity
LEFT JOIN failures x ON x.entity = r.entity AND x.source_file = f.source_file AND x.check_name = r.check_name
WHERE r.level = 'row'
ORDER BY r.check_order, f.source_file;
