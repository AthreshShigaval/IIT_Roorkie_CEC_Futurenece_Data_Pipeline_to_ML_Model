-- =============================================================================
-- mart_category_margin.sql : business question 2
-- margin = revenue - quantity x cost_price, delivered orders only
-- =============================================================================
CREATE OR REPLACE TABLE mart_category_margin AS
SELECT
    p.category,
    count(*) AS delivered_orders,
    sum(f.quantity) AS units,
    round(sum(f.net_revenue), 2) AS net_revenue,
    round(sum(f.cost_amount), 2) AS cost_amount,
    round(sum(f.gross_margin), 2) AS gross_margin,
    round(100 * sum(f.gross_margin) / sum(f.net_revenue), 2) AS margin_pct
FROM mart_fact_orders f
LEFT JOIN stg_products p ON p.product_id = f.product_id AND p.dq_status = 'PASS'
WHERE f.order_status = 'DELIVERED'
GROUP BY ALL
ORDER BY net_revenue DESC;
