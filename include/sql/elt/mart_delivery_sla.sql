-- =============================================================================
-- mart_delivery_sla.sql : business question 3, the "10-minute promise" KPI
-- =============================================================================
CREATE OR REPLACE TABLE mart_delivery_sla AS
SELECT
    f.store_id, s.store_name, s.zone,
    count(*) AS delivered_orders,
    round(avg(f.delivery_minutes), 2) AS avg_delivery_minutes,
    median(f.delivery_minutes) AS median_delivery_minutes,
    count(*) FILTER (WHERE f.within_15_min) AS within_15_min_orders,
    round(100.0 * count(*) FILTER (WHERE f.within_15_min) / count(*), 2) AS pct_within_15_min
FROM mart_fact_orders f
LEFT JOIN stg_stores s ON s.store_id = f.store_id AND s.dq_status = 'PASS'
WHERE f.order_status = 'DELIVERED'
GROUP BY ALL
ORDER BY f.store_id;
