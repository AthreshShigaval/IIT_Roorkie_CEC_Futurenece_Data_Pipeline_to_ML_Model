-- =============================================================================
-- dq_table_checks.sql : file-level checks on the orders files of this run
--   volume      - more than a 90% drop vs the previous run of the same file
--   price drift - a store's average unit_price > 3x the median store
--   capacity    - PASS orders per store per day vs capacity_orders_per_day
--   freshness   - latest order_ts vs the file's business date and vs run time
-- =============================================================================
DELETE FROM dq_results
WHERE run_id = getvariable('run_id') AND dag_id = getvariable('dag_id')
  AND check_name IN (SELECT check_name FROM dq_check_registry WHERE level = 'table' AND entity = 'orders'
                     AND check_name <> 'orders_schema_columns');

INSERT INTO dq_results (run_id, dag_id, check_name, dimension, severity, rows_checked, rows_failed, status,
                        checked_at, entity, source_file, details)
WITH b AS (
    SELECT * FROM stg_orders_batch WHERE _run_id = getvariable('run_id')
),
files AS (
    SELECT _source_file AS source_file, count(*) AS n, batch_cutoff(_source_file) AS cutoff FROM b GROUP BY ALL
),
prev AS (
    SELECT source_file, rows_checked FROM (
        SELECT source_file, rows_checked,
               row_number() OVER (PARTITION BY source_file ORDER BY checked_at DESC) AS rn
        FROM dq_results
        WHERE dag_id = getvariable('dag_id') AND check_name = 'orders_volume_vs_previous'
          AND run_id <> getvariable('run_id')
    ) WHERE rn = 1
),
volume AS (
    SELECT 'orders_volume_vs_previous' AS check_name, f.source_file, f.n AS rows_checked,
           CASE WHEN p.rows_checked IS NOT NULL
                 AND f.n < p.rows_checked * (1 - getvariable('volume_drop_threshold')) THEN 1 ELSE 0 END AS rows_failed,
           'rows=' || f.n || ' previous_run_rows=' || coalesce(CAST(p.rows_checked AS VARCHAR), 'None') AS details
    FROM files f LEFT JOIN prev p USING (source_file)
),
store_avg AS (
    SELECT _source_file AS source_file, store_id, avg(unit_price) AS avg_price, count(*) AS n
    FROM b WHERE store_id IS NOT NULL AND unit_price IS NOT NULL GROUP BY ALL
),
store_median AS (
    SELECT source_file, median(avg_price) AS median_price FROM store_avg GROUP BY ALL
),
price AS (
    SELECT 'orders_store_price_outlier', f.source_file, f.n,
           coalesce(sum(s.n) FILTER (WHERE s.avg_price > getvariable('price_outlier_factor') * m.median_price), 0),
           'median_store_avg_price=' || coalesce(round(m.median_price, 2), 0) || ' outlier_stores='
               || coalesce(CAST(list_sort(list(s.store_id) FILTER (
                      WHERE s.avg_price > getvariable('price_outlier_factor') * m.median_price)) AS VARCHAR), '[]')
    FROM files f
    LEFT JOIN store_median m USING (source_file)
    LEFT JOIN store_avg s USING (source_file)
    GROUP BY f.source_file, f.n, m.median_price
),
daily AS (
    SELECT b._source_file AS source_file, b.store_id, b.order_date, count(*) AS orders,
           any_value(s.capacity_orders_per_day) AS capacity
    FROM b JOIN stg_stores s ON s.store_id = b.store_id AND s.dq_status = 'PASS' AND s.capacity_orders_per_day > 0
    WHERE b.dq_status = 'PASS'
    GROUP BY ALL
),
capacity AS (
    SELECT 'orders_store_daily_capacity', f.source_file, count(d.store_id),
           count(*) FILTER (WHERE d.orders > d.capacity),
           'store_days_over_capacity=' || count(*) FILTER (WHERE d.orders > d.capacity)
    FROM files f LEFT JOIN daily d USING (source_file)
    GROUP BY f.source_file
),
latest AS (
    SELECT f.source_file, f.n, f.cutoff, max(b.order_ts) FILTER (WHERE b.order_ts < f.cutoff) AS latest_ts
    FROM files f LEFT JOIN b ON b._source_file = f.source_file
    GROUP BY ALL
),
fresh_batch AS (
    SELECT 'orders_fresh_vs_batch', source_file, n,
           CASE WHEN latest_ts IS NULL OR latest_ts < cutoff - INTERVAL 1 DAY THEN 1 ELSE 0 END,
           'latest_order_ts=' || coalesce(CAST(latest_ts AS VARCHAR), 'None')
               || ' business_date=' || CAST(CAST(cutoff - INTERVAL 1 DAY AS DATE) AS VARCHAR)
    FROM latest
),
fresh_run AS (
    SELECT 'orders_fresh_vs_run', source_file, n,
           CASE WHEN latest_ts IS NULL OR latest_ts < CAST(getvariable('run_time_ist') AS TIMESTAMP)
                     - to_days(CAST(getvariable('freshness_max_lag_days') AS INTEGER)) THEN 1 ELSE 0 END,
           'latest_order_ts=' || coalesce(CAST(latest_ts AS VARCHAR), 'None')
               || ' run_time_ist=' || getvariable('run_time_ist')
               || ' lag_days=' || coalesce(CAST(round(epoch(CAST(getvariable('run_time_ist') AS TIMESTAMP) - latest_ts)
                                              / 86400, 1) AS VARCHAR), 'None')
    FROM latest
),
results AS (
    SELECT * FROM volume
    UNION ALL SELECT * FROM price
    UNION ALL SELECT * FROM capacity
    UNION ALL SELECT * FROM fresh_batch
    UNION ALL SELECT * FROM fresh_run
)
SELECT
    getvariable('run_id'), getvariable('dag_id'), r.check_name, g.dimension, g.severity,
    r.rows_checked, r.rows_failed,
    CASE WHEN r.rows_failed = 0 THEN 'PASS' WHEN g.severity = 'Critical' THEN 'FAIL' ELSE 'WARN' END,
    CAST(getvariable('now_utc') AS TIMESTAMP), g.entity, r.source_file, r.details
FROM results r JOIN dq_check_registry g USING (check_name)
ORDER BY g.check_order, r.source_file;
