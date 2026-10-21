-- =============================================================================
-- mart_payment_reconciliation.sql : one row per order / payment mismatch
-- (same rules as reconcile() in include/nammamart/etl_transform.py).
-- Order-side checks only cover orders placed before the payment export ends.
-- =============================================================================
CREATE OR REPLACE TABLE mart_payment_reconciliation AS
WITH known_orders AS (
    SELECT DISTINCT order_id FROM stg_orders_batch
    WHERE _run_id = getvariable('run_id') AND order_id IS NOT NULL
),
uniq AS (SELECT * FROM stg_payments WHERE NOT is_duplicate),
payment_side AS (
    SELECT 'DUPLICATE_PAYMENT_RECORD' AS mismatch_reason, p.order_id, p.payment_id AS payment_ids,
           NULL::VARCHAR AS order_status, p.payment_mode, NULL::DOUBLE AS order_amount, p.amount_paid,
           NULL::DOUBLE AS difference, p.amount_paid AS unreconciled_amount, NULL::DATE AS order_date,
           NULL::VARCHAR AS store_id
    FROM stg_payments p WHERE p.is_duplicate
    UNION ALL
    SELECT CASE WHEN k.order_id IS NULL THEN 'PAYMENT_WITHOUT_ORDER' ELSE 'PAYMENT_FOR_REJECTED_ORDER' END,
           u.order_id, u.payment_id, NULL, u.payment_mode, NULL, u.amount_paid, NULL, u.amount_paid, NULL, NULL
    FROM uniq u
    LEFT JOIN known_orders k ON k.order_id = u.order_id
    LEFT JOIN mart_fact_orders c ON c.order_id = u.order_id
    WHERE k.order_id IS NULL OR c.order_id IS NULL
),
agg AS (
    SELECT order_id, count(*) AS n_any,
           count(*) FILTER (WHERE payment_status = 'SUCCESS') AS n_success,
           count(*) FILTER (WHERE payment_status = 'REFUNDED') AS n_refund,
           coalesce(sum(amount_paid) FILTER (WHERE payment_status = 'SUCCESS'), 0) AS success_amount,
           string_agg(payment_id, ',' ORDER BY payment_id) AS payment_ids
    FROM uniq WHERE order_id IS NOT NULL GROUP BY order_id
),
order_side AS (
    SELECT
        CASE
            WHEN c.order_status = 'DELIVERED' AND coalesce(a.n_any, 0) = 0 THEN 'MISSING_PAYMENT'
            WHEN c.order_status = 'DELIVERED' AND coalesce(a.n_success, 0) = 0 THEN 'PAYMENT_NOT_SUCCESSFUL'
            WHEN c.order_status = 'DELIVERED'
             AND abs(c.order_amount - coalesce(a.success_amount, 0)) > getvariable('amount_tolerance')
                THEN 'AMOUNT_MISMATCH'
            WHEN c.order_status = 'RETURNED' AND a.n_success > 0 AND a.n_refund = 0 THEN 'REFUND_NOT_RECORDED'
            WHEN c.order_status = 'CANCELLED' AND a.n_success > 0 THEN 'PAYMENT_ON_CANCELLED_ORDER'
        END AS mismatch_reason,
        c.order_id, a.payment_ids, c.order_status, c.payment_mode, c.order_amount,
        coalesce(a.success_amount, 0) AS success_amount,
        c.order_amount - coalesce(a.success_amount, 0) AS difference,
        c.order_date, c.store_id
    FROM mart_fact_orders c
    LEFT JOIN agg a ON a.order_id = c.order_id
    WHERE c.order_ts < CAST(getvariable('payments_coverage_end') AS TIMESTAMP)
),
order_rows AS (
    SELECT mismatch_reason, order_id, payment_ids, order_status, payment_mode, order_amount,
           CASE WHEN mismatch_reason IN ('MISSING_PAYMENT', 'PAYMENT_NOT_SUCCESSFUL') THEN 0
                ELSE success_amount END AS amount_paid,
           difference,
           CASE WHEN mismatch_reason IN ('MISSING_PAYMENT', 'PAYMENT_NOT_SUCCESSFUL') THEN order_amount
                WHEN mismatch_reason = 'AMOUNT_MISMATCH' THEN abs(difference)
                ELSE success_amount END AS unreconciled_amount,
           order_date, store_id
    FROM order_side
    WHERE mismatch_reason IS NOT NULL
)
SELECT mismatch_reason, order_id, payment_ids, order_status, payment_mode,
       round(order_amount, 2) AS order_amount, round(amount_paid, 2) AS amount_paid,
       round(difference, 2) AS difference, round(unreconciled_amount, 2) AS unreconciled_amount,
       order_date, store_id, getvariable('run_id') AS _run_id,
       CAST(getvariable('now_utc') AS TIMESTAMP) AS _reconciled_at
FROM (SELECT * FROM payment_side UNION ALL SELECT * FROM order_rows)
ORDER BY mismatch_reason, order_id, payment_ids;
