-- =============================================================================
-- stg_payments.sql : this run's raw_payments -> typed, standardised, checked
-- against the orders received in this run (orders.csv + increment files).
-- =============================================================================
CREATE OR REPLACE TABLE stg_payments AS
WITH order_ref AS (
    -- latest version of each order seen this run: newest business date, first line in the file
    SELECT order_id, order_amount, order_ts FROM (
        SELECT nz(order_id) AS order_id, to_num(order_amount) AS order_amount, to_ts(order_ts) AS order_ts,
               row_number() OVER (PARTITION BY nz(order_id) ORDER BY batch_date(_source_file) DESC, _source_line) AS rn
        FROM raw_orders
        WHERE _run_id = getvariable('run_id') AND nz(order_id) IS NOT NULL
    ) WHERE rn = 1
),
typed AS (
    SELECT
        nz(payment_id) AS payment_id, nz(order_id) AS order_id,
        nz(payment_ts) AS payment_ts_raw, to_ts(payment_ts) AS payment_ts,
        std_payment_mode(payment_mode) AS payment_mode, payment_mode AS payment_mode_src,
        to_num(amount_paid) AS amount_paid, upper(nz(payment_status)) AS payment_status,
        nz(gateway_ref) AS gateway_ref, _source_file, _source_line, _run_id
    FROM raw_payments
    WHERE _run_id = getvariable('run_id')
),
enriched AS (
    SELECT t.*,
        row_number() OVER (PARTITION BY t.payment_id ORDER BY t._source_line) AS rn,
        o.order_id IS NOT NULL AS order_found, o.order_amount AS ref_order_amount, o.order_ts AS ref_order_ts
    FROM typed t LEFT JOIN order_ref o ON o.order_id = t.order_id
),
flagged AS (
    SELECT *,
        list_filter([
            CASE WHEN payment_id IS NULL THEN 'payments_payment_id_not_null' END,
            CASE WHEN order_id IS NULL THEN 'payments_order_id_not_null' END,
            CASE WHEN payment_mode IS DISTINCT FROM 'Cash on Delivery' AND gateway_ref IS NULL
                 THEN 'payments_gateway_ref_present' END,
            CASE WHEN payment_id IS NOT NULL AND rn > 1 THEN 'payments_payment_id_unique' END,
            CASE WHEN payment_status IS NULL OR payment_status NOT IN ('SUCCESS', 'FAILED', 'REFUNDED')
                 THEN 'payments_status_valid' END,
            CASE WHEN payment_mode IS NULL THEN 'payments_payment_mode_valid' END,
            CASE WHEN payment_ts_raw IS NOT NULL AND payment_ts IS NULL THEN 'payments_ts_format' END,
            CASE WHEN amount_paid IS NULL THEN 'payments_amount_numeric' END,
            CASE WHEN payment_status = 'SUCCESS' AND amount_paid <= 0 THEN 'payments_amount_positive' END,
            CASE WHEN order_id IS NOT NULL AND NOT order_found THEN 'payments_order_fk' END,
            CASE WHEN payment_status = 'SUCCESS' AND abs(amount_paid - ref_order_amount) > getvariable('amount_tolerance')
                 THEN 'payments_amount_matches_order' END,
            CASE WHEN payment_mode IS NOT NULL AND payment_mode_src <> payment_mode THEN 'payments_mode_casing' END,
            CASE WHEN payment_ts >= batch_cutoff(_source_file) THEN 'payments_ts_not_future' END,
            CASE WHEN payment_ts < ref_order_ts THEN 'payments_ts_after_order' END
        ], lambda x: x IS NOT NULL) AS failed_checks
    FROM enriched
)
SELECT
    payment_id, order_id, payment_ts, payment_mode, amount_paid, payment_status, gateway_ref,
    payment_id IS NOT NULL AND rn > 1 AS is_duplicate,
    dq_status_of(failed_checks) AS dq_status,
    dq_reasons(failed_checks, 'Critical') AS failure_reason,
    dq_reasons(failed_checks, 'Warning') AS dq_warnings,
    failed_checks, _source_file, _source_line, _run_id
FROM flagged
ORDER BY _source_line;
