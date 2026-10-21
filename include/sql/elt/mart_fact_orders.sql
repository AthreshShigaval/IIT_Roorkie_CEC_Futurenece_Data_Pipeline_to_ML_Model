-- =============================================================================
-- mart_fact_orders.sql : the ELT equivalent of etl_fact_orders.
-- A view over PASS rows of stg_orders, enriched with product cost and the
-- derived measures every mart uses. Always reflects the latest MERGE.
-- =============================================================================
CREATE OR REPLACE VIEW mart_fact_orders AS
SELECT
    o.order_id, o.order_ts, o.order_date, o.customer_id, o.product_id, o.store_id,
    o.quantity, o.unit_price, o.discount_pct, o.order_amount, o.payment_mode, o.order_status, o.channel,
    o.delivery_minutes, p.cost_price,
    CASE WHEN o.order_status = 'DELIVERED' THEN o.order_amount ELSE 0 END AS net_revenue,
    CASE WHEN o.order_status = 'DELIVERED' THEN o.quantity * p.cost_price ELSE 0 END AS cost_amount,
    CASE WHEN o.order_status = 'DELIVERED' THEN o.order_amount - o.quantity * p.cost_price ELSE 0 END AS gross_margin,
    CASE WHEN o.order_status = 'DELIVERED' THEN o.delivery_minutes <= 15 END AS within_15_min,
    o.dq_warnings, o._batch_date, o._source_file, o._source_line, o._run_id
FROM stg_orders o
LEFT JOIN stg_products p ON p.product_id = o.product_id AND p.dq_status = 'PASS'
WHERE o.dq_status = 'PASS';
