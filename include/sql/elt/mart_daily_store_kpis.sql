-- =============================================================================
-- mart_daily_store_kpis.sql : one row per order_date x store.
-- Business question 1 (net revenue by zone / store) and the daily view of Q3.
-- =============================================================================
CREATE OR REPLACE TABLE mart_daily_store_kpis AS
SELECT
    f.order_date, s.zone, f.store_id, s.store_name,
    count(*) AS total_orders,
    count(*) FILTER (WHERE f.order_status = 'DELIVERED') AS delivered_orders,
    count(*) FILTER (WHERE f.order_status = 'CANCELLED') AS cancelled_orders,
    count(*) FILTER (WHERE f.order_status = 'RETURNED') AS returned_orders,
    round(sum(f.net_revenue), 2) AS net_revenue,
    round(sum(f.gross_margin), 2) AS gross_margin,
    round(avg(f.delivery_minutes) FILTER (WHERE f.order_status = 'DELIVERED'), 2) AS avg_delivery_minutes,
    round(100.0 * count(*) FILTER (WHERE f.within_15_min)
          / nullif(count(*) FILTER (WHERE f.order_status = 'DELIVERED'), 0), 2) AS pct_within_15_min
FROM mart_fact_orders f
LEFT JOIN stg_stores s ON s.store_id = f.store_id AND s.dq_status = 'PASS'
GROUP BY ALL
ORDER BY f.order_date, s.zone, f.store_id;
