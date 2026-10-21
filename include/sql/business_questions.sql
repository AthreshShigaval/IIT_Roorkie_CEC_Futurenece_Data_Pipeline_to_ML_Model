-- =============================================================================
-- business_questions.sql : the six client questions, each answered by one
-- simple query per pipeline (ETL star schema vs ELT marts).
-- Run all of them: python tools/business_questions.py
-- Each block starts with "-- name: <id> | <title>".
-- =============================================================================

-- name: q1_etl_zone | Q1 (ETL) Net revenue by zone, delivered orders after discount
SELECT zone, sum(delivered_orders) AS delivered_orders, round(sum(net_revenue), 2) AS net_revenue
FROM etl_daily_zone_revenue
GROUP BY zone ORDER BY net_revenue DESC;

-- name: q1_elt_zone | Q1 (ELT) Net revenue by zone, delivered orders after discount
SELECT zone, sum(delivered_orders) AS delivered_orders, round(sum(net_revenue), 2) AS net_revenue
FROM mart_daily_store_kpis
GROUP BY zone ORDER BY net_revenue DESC;

-- name: q1_etl_store | Q1 (ETL) Net revenue by store
SELECT store_id, store_name, zone, sum(delivered_orders) AS delivered_orders, round(sum(net_revenue), 2) AS net_revenue
FROM etl_daily_zone_revenue
GROUP BY ALL ORDER BY store_id;

-- name: q1_elt_store | Q1 (ELT) Net revenue by store
SELECT store_id, store_name, zone, sum(delivered_orders) AS delivered_orders, round(sum(net_revenue), 2) AS net_revenue
FROM mart_daily_store_kpis
GROUP BY ALL ORDER BY store_id;

-- name: q1_etl_daily | Q1 (ETL) Daily net revenue by zone, last 3 days of September
SELECT order_date, zone, round(sum(net_revenue), 2) AS net_revenue
FROM etl_daily_zone_revenue
WHERE order_date BETWEEN DATE '2026-09-28' AND DATE '2026-09-30'
GROUP BY ALL ORDER BY order_date, zone;

-- name: q1_elt_daily | Q1 (ELT) Daily net revenue by zone, last 3 days of September
SELECT order_date, zone, round(sum(net_revenue), 2) AS net_revenue
FROM mart_daily_store_kpis
WHERE order_date BETWEEN DATE '2026-09-28' AND DATE '2026-09-30'
GROUP BY ALL ORDER BY order_date, zone;

-- name: q2_etl | Q2 (ETL) Revenue and gross margin by category
SELECT category, net_revenue, cost_amount, gross_margin, margin_pct FROM etl_category_margin ORDER BY net_revenue DESC;

-- name: q2_elt | Q2 (ELT) Revenue and gross margin by category
SELECT category, net_revenue, cost_amount, gross_margin, margin_pct FROM mart_category_margin ORDER BY net_revenue DESC;

-- name: q3_etl | Q3 (ETL) Average delivery time per store and % delivered within 15 minutes
SELECT f.store_id, s.store_name, count(*) AS delivered_orders,
       round(avg(f.delivery_minutes), 2) AS avg_delivery_minutes,
       round(100.0 * count(*) FILTER (WHERE f.within_15_min) / count(*), 2) AS pct_within_15_min
FROM etl_fact_orders f JOIN etl_dim_store s USING (store_id)
WHERE f.order_status = 'DELIVERED'
GROUP BY ALL ORDER BY f.store_id;

-- name: q3_elt | Q3 (ELT) Average delivery time per store and % delivered within 15 minutes
SELECT store_id, store_name, delivered_orders, avg_delivery_minutes, pct_within_15_min
FROM mart_delivery_sla ORDER BY store_id;

-- name: q4_etl | Q4 (ETL) Revenue by payment mode and unreconciled amount
SELECT f.payment_mode, count(*) AS delivered_orders, round(sum(f.net_revenue), 2) AS net_revenue,
       (SELECT round(sum(unreconciled_amount), 2) FROM etl_payment_reconciliation r
         WHERE r.payment_mode = f.payment_mode) AS unreconciled_amount
FROM etl_fact_orders f WHERE f.order_status = 'DELIVERED'
GROUP BY ALL ORDER BY net_revenue DESC;

-- name: q4_elt | Q4 (ELT) Revenue by payment mode and unreconciled amount
SELECT payment_mode, delivered_orders, net_revenue, unreconciled_amount FROM mart_payment_mode_summary
ORDER BY net_revenue DESC;

-- name: q4_etl_total | Q4 (ETL) Total that does not reconcile with the payment gateway, by reason
SELECT mismatch_reason, count(*) AS rows, round(sum(unreconciled_amount), 2) AS unreconciled_amount
FROM etl_payment_reconciliation GROUP BY ROLLUP (mismatch_reason) ORDER BY mismatch_reason NULLS LAST;

-- name: q4_elt_total | Q4 (ELT) Total that does not reconcile with the payment gateway, by reason
SELECT mismatch_reason, count(*) AS rows, round(sum(unreconciled_amount), 2) AS unreconciled_amount
FROM mart_payment_reconciliation GROUP BY ROLLUP (mismatch_reason) ORDER BY mismatch_reason NULLS LAST;

-- name: q5_etl | Q5 (ETL) Top 10 customers by spend (masked)
SELECT c.customer_id, c.customer_name_masked, c.email_masked, c.phone_masked, c.loyalty_tier,
       count(*) AS delivered_orders, round(sum(f.net_revenue), 2) AS total_spend
FROM etl_fact_orders f JOIN etl_dim_customer c USING (customer_id)
WHERE f.order_status = 'DELIVERED'
GROUP BY ALL ORDER BY total_spend DESC, c.customer_id LIMIT 10;

-- name: q5_elt | Q5 (ELT) Top 10 customers by spend (masked)
SELECT customer_id, customer_name_masked, email_masked, phone_masked, loyalty_tier, delivered_orders, total_spend
FROM mart_top_customers WHERE spend_rank <= 10 ORDER BY spend_rank;

-- name: q6_etl | Q6 (ETL) Rows rejected in the latest run, by file and reason
SELECT source_file, split_part(reason, ':', 1) AS failed_check, count(*) AS rows_rejected
FROM (SELECT source_file, unnest(string_split(failure_reason, '; ')) AS reason FROM etl_quarantine
      WHERE run_id = (SELECT run_id FROM pipeline_runs WHERE dag_id = 'nammamart_etl_athresh'
                      ORDER BY end_time DESC LIMIT 1))
GROUP BY ALL ORDER BY source_file, rows_rejected DESC, failed_check;

-- name: q6_elt | Q6 (ELT) Rows rejected in the latest run, by file and reason
SELECT source_file, split_part(reason, ':', 1) AS failed_check, count(*) AS rows_rejected
FROM (SELECT source_file, unnest(string_split(failure_reason, '; ')) AS reason FROM elt_quarantine
      WHERE run_id = (SELECT run_id FROM pipeline_runs WHERE dag_id = 'nammamart_elt_athresh'
                      ORDER BY end_time DESC LIMIT 1))
GROUP BY ALL ORDER BY source_file, rows_rejected DESC, failed_check;
