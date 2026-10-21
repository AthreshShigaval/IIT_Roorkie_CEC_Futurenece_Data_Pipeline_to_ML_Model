-- =============================================================================
-- mart_payment_mode_summary.sql : business question 4
-- revenue by payment mode, plus the amount that does not reconcile
-- =============================================================================
CREATE OR REPLACE TABLE mart_payment_mode_summary AS
WITH revenue AS (
    SELECT payment_mode, count(*) AS delivered_orders, round(sum(net_revenue), 2) AS net_revenue
    FROM mart_fact_orders WHERE order_status = 'DELIVERED' GROUP BY ALL
),
gaps AS (
    SELECT payment_mode, count(*) AS mismatches, round(sum(unreconciled_amount), 2) AS unreconciled_amount
    FROM mart_payment_reconciliation GROUP BY ALL
)
SELECT coalesce(r.payment_mode, g.payment_mode) AS payment_mode,
       coalesce(r.delivered_orders, 0) AS delivered_orders, coalesce(r.net_revenue, 0) AS net_revenue,
       coalesce(g.mismatches, 0) AS mismatches, coalesce(g.unreconciled_amount, 0) AS unreconciled_amount
FROM revenue r FULL OUTER JOIN gaps g ON g.payment_mode IS NOT DISTINCT FROM r.payment_mode
ORDER BY net_revenue DESC;
