-- =============================================================================
-- mart_top_customers.sql : business question 5. Masked identity only:
-- no raw name, email or phone ever reaches the mart layer.
-- =============================================================================
CREATE OR REPLACE TABLE mart_top_customers AS
WITH spend AS (
    SELECT customer_id, count(*) AS delivered_orders, round(sum(net_revenue), 2) AS total_spend
    FROM mart_fact_orders
    WHERE order_status = 'DELIVERED'
    GROUP BY customer_id
)
SELECT
    rank() OVER (ORDER BY s.total_spend DESC, s.customer_id) AS spend_rank,
    s.customer_id, c.customer_name_masked, c.email_masked, c.phone_masked, c.loyalty_tier,
    s.delivered_orders, s.total_spend
FROM spend s
LEFT JOIN stg_customers c ON c.customer_id = s.customer_id AND c.dq_status = 'PASS'
ORDER BY spend_rank;
