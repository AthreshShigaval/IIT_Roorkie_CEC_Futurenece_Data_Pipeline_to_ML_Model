-- =============================================================================
-- stg_orders.sql : evaluate ONE orders file from this run's raw_orders, then
-- MERGE it into stg_orders on order_id.  Used for orders.csv and, by the
-- merge_increments task, for every orders_<date>_increment.csv.
--   variables: run_id, source_file, now_utc, base_batch_date, thresholds
-- MERGE rule: a row is replaced only by a version from the same or a newer
-- business date, and a FAIL version never overwrites a PASS version.
-- =============================================================================

-- 1. Evaluate every row of the file (duplicates included) into stg_orders_batch
DELETE FROM stg_orders_batch
WHERE _run_id = getvariable('run_id') AND _source_file = getvariable('source_file');

INSERT INTO stg_orders_batch BY NAME
WITH typed AS (
    SELECT
        nz(order_id) AS order_id, nz(order_ts) AS order_ts_raw, to_ts(order_ts) AS order_ts,
        nz(customer_id) AS customer_id, nz(store_id) AS store_id, nz(product_id) AS product_id,
        to_num(quantity) AS quantity, to_num(unit_price) AS unit_price, to_num(discount_pct) AS discount_pct,
        to_num(order_amount) AS order_amount,
        to_num(delivery_minutes) AS delivery_minutes, nz(delivery_minutes) AS delivery_minutes_raw,
        std_payment_mode(payment_mode) AS payment_mode, payment_mode AS payment_mode_src,
        upper(nz(order_status)) AS order_status, order_status AS order_status_src,
        nz(channel) AS channel, _source_file, _source_line, _run_id, batch_date(_source_file) AS _batch_date
    FROM raw_orders
    WHERE _run_id = getvariable('run_id') AND _source_file = getvariable('source_file')
),
enriched AS (
    SELECT t.*,
        row_number() OVER (PARTITION BY t.order_id ORDER BY t._source_line) AS rn,
        c.customer_id IS NOT NULL AS customer_found,
        p.product_id IS NOT NULL AS product_found,
        s.store_id IS NOT NULL AS store_found,
        p.mrp
    FROM typed t
    LEFT JOIN (SELECT customer_id FROM stg_customers WHERE dq_status = 'PASS') c ON c.customer_id = t.customer_id
    LEFT JOIN (SELECT product_id, mrp FROM stg_products WHERE dq_status = 'PASS') p ON p.product_id = t.product_id
    LEFT JOIN (SELECT store_id FROM stg_stores WHERE dq_status = 'PASS') s ON s.store_id = t.store_id
),
flagged AS (
    SELECT *,
        list_filter([
            -- Completeness
            CASE WHEN order_id IS NULL THEN 'orders_order_id_not_null' END,
            CASE WHEN customer_id IS NULL THEN 'orders_customer_id_not_null' END,
            CASE WHEN order_ts_raw IS NULL THEN 'orders_order_ts_not_null' END,
            CASE WHEN order_status = 'DELIVERED' AND delivery_minutes_raw IS NULL THEN 'orders_delivered_has_minutes' END,
            -- Uniqueness
            CASE WHEN order_id IS NOT NULL AND rn > 1 THEN 'orders_order_id_unique' END,
            -- Validity
            CASE WHEN order_ts_raw IS NOT NULL AND order_ts IS NULL THEN 'orders_order_ts_format' END,
            CASE WHEN order_status IS NULL OR order_status NOT IN ('DELIVERED', 'CANCELLED', 'RETURNED')
                 THEN 'orders_status_valid' END,
            CASE WHEN payment_mode IS NULL THEN 'orders_payment_mode_valid' END,
            CASE WHEN quantity IS NULL OR unit_price IS NULL OR discount_pct IS NULL OR order_amount IS NULL
                   OR (delivery_minutes_raw IS NOT NULL AND delivery_minutes IS NULL) THEN 'orders_numeric_valid' END,
            CASE WHEN discount_pct < 0 OR discount_pct > 100 THEN 'orders_discount_range' END,
            -- Accuracy
            CASE WHEN quantity <= 0 THEN 'orders_quantity_positive' END,
            CASE WHEN abs(quantity * unit_price * (1 - discount_pct / 100) - order_amount) > getvariable('amount_tolerance')
                 THEN 'orders_amount_formula' END,
            CASE WHEN delivery_minutes <= 0 OR delivery_minutes > getvariable('max_delivery_minutes')
                 THEN 'orders_delivery_under_max' END,
            CASE WHEN delivery_minutes > 0 AND delivery_minutes <= getvariable('max_delivery_minutes')
                  AND (delivery_minutes < getvariable('normal_delivery_min') OR delivery_minutes > getvariable('normal_delivery_max'))
                 THEN 'orders_delivery_normal_range' END,
            CASE WHEN unit_price > mrp THEN 'orders_unit_price_within_mrp' END,
            -- Consistency
            CASE WHEN customer_id IS NOT NULL AND NOT customer_found THEN 'orders_customer_fk' END,
            CASE WHEN NOT product_found THEN 'orders_product_fk' END,
            CASE WHEN NOT store_found THEN 'orders_store_fk' END,
            CASE WHEN order_status IN ('DELIVERED', 'CANCELLED', 'RETURNED') AND order_status_src <> order_status
                 THEN 'orders_status_casing' END,
            CASE WHEN payment_mode IS NOT NULL AND payment_mode_src <> payment_mode THEN 'orders_payment_mode_casing' END,
            -- Timeliness
            CASE WHEN order_ts >= batch_cutoff(_source_file) THEN 'orders_ts_not_future' END
        ], lambda x: x IS NOT NULL) AS failed_checks
    FROM enriched
)
SELECT
    order_id, order_ts, CAST(order_ts AS DATE) AS order_date, customer_id, product_id, store_id,
    try_cast(quantity AS INTEGER) AS quantity, unit_price, discount_pct, order_amount, payment_mode, order_status, channel,
    try_cast(delivery_minutes AS INTEGER) AS delivery_minutes,
    dq_status_of(failed_checks) AS dq_status,
    dq_reasons(failed_checks, 'Critical') AS failure_reason,
    dq_reasons(failed_checks, 'Warning') AS dq_warnings,
    failed_checks,
    order_id IS NOT NULL AND rn > 1 AS is_duplicate,
    _batch_date, _source_file, _source_line, _run_id,
    CAST(getvariable('now_utc') AS TIMESTAMP) AS _evaluated_at
FROM flagged;

-- 2. Rows that take part in the MERGE: one per order_id (duplicate copies and missing ids stay in the batch log)
CREATE OR REPLACE TEMP VIEW merge_src AS
SELECT * FROM stg_orders_batch
WHERE _run_id = getvariable('run_id') AND _source_file = getvariable('source_file')
  AND order_id IS NOT NULL AND NOT is_duplicate;

-- 3. Record what the MERGE is about to do (inserted / updated / changed / skipped)
DELETE FROM elt_merge_log WHERE run_id = getvariable('run_id') AND source_file = getvariable('source_file');
INSERT INTO elt_merge_log
SELECT
    getvariable('run_id'), getvariable('source_file'),
    (SELECT count(*) FROM stg_orders_batch
      WHERE _run_id = getvariable('run_id') AND _source_file = getvariable('source_file')),
    count(*) FILTER (WHERE t.order_id IS NULL),
    count(*) FILTER (WHERE t.order_id IS NOT NULL AND s._batch_date >= t._batch_date
                       AND (s.dq_status = 'PASS' OR t.dq_status = 'FAIL')),
    count(*) FILTER (WHERE t.order_id IS NOT NULL AND s._batch_date >= t._batch_date
                       AND (s.dq_status = 'PASS' OR t.dq_status = 'FAIL')
                       AND (s.order_status IS DISTINCT FROM t.order_status OR s.order_amount IS DISTINCT FROM t.order_amount
                            OR s.order_ts IS DISTINCT FROM t.order_ts OR s.quantity IS DISTINCT FROM t.quantity
                            OR s.delivery_minutes IS DISTINCT FROM t.delivery_minutes
                            OR s.dq_status IS DISTINCT FROM t.dq_status)),
    count(*) FILTER (WHERE t.order_id IS NOT NULL AND NOT (s._batch_date >= t._batch_date
                       AND (s.dq_status = 'PASS' OR t.dq_status = 'FAIL'))),
    count(*) FILTER (WHERE s.dq_status = 'PASS' AND (t.order_id IS NULL OR (s._batch_date >= t._batch_date))),
    CAST(getvariable('now_utc') AS TIMESTAMP)
FROM merge_src s LEFT JOIN stg_orders t ON t.order_id = s.order_id;

-- 4. MERGE / upsert on order_id
MERGE INTO stg_orders t
USING merge_src s ON t.order_id = s.order_id
WHEN MATCHED AND s._batch_date >= t._batch_date AND (s.dq_status = 'PASS' OR t.dq_status = 'FAIL') THEN UPDATE SET
    order_ts = s.order_ts, order_date = s.order_date, customer_id = s.customer_id, product_id = s.product_id,
    store_id = s.store_id, quantity = s.quantity, unit_price = s.unit_price, discount_pct = s.discount_pct,
    order_amount = s.order_amount, payment_mode = s.payment_mode, order_status = s.order_status,
    channel = s.channel, delivery_minutes = s.delivery_minutes, dq_status = s.dq_status,
    failure_reason = s.failure_reason, dq_warnings = s.dq_warnings, failed_checks = s.failed_checks,
    _batch_date = s._batch_date, _source_file = s._source_file, _source_line = s._source_line,
    _run_id = s._run_id, _merged_at = CAST(getvariable('now_utc') AS TIMESTAMP)
WHEN NOT MATCHED THEN INSERT (
    order_id, order_ts, order_date, customer_id, product_id, store_id, quantity, unit_price, discount_pct,
    order_amount, payment_mode, order_status, channel, delivery_minutes, dq_status, failure_reason, dq_warnings,
    failed_checks, _batch_date, _source_file, _source_line, _run_id, _merged_at
) VALUES (
    s.order_id, s.order_ts, s.order_date, s.customer_id, s.product_id, s.store_id, s.quantity, s.unit_price,
    s.discount_pct, s.order_amount, s.payment_mode, s.order_status, s.channel, s.delivery_minutes, s.dq_status,
    s.failure_reason, s.dq_warnings, s.failed_checks, s._batch_date, s._source_file, s._source_line, s._run_id,
    CAST(getvariable('now_utc') AS TIMESTAMP)
);
