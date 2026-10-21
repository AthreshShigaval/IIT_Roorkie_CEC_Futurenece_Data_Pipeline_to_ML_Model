-- =============================================================================
-- elt_quarantine.sql : every row of this run that failed a Critical check,
-- with the raw record exactly as received (from the raw layer) and the reason.
-- The rows also stay in raw_* (never deleted) and in staging with dq_status = 'FAIL'.
-- =============================================================================
DELETE FROM elt_quarantine WHERE run_id = getvariable('run_id');

INSERT INTO elt_quarantine (run_id, source_file, source_line, record_key, failure_reason, raw_record, quarantined_at)
SELECT s._run_id, s._source_file, s._source_line, r.order_id, s.failure_reason,
       to_json({order_id: r.order_id, order_ts: r.order_ts, customer_id: r.customer_id, store_id: r.store_id,
                product_id: r.product_id, quantity: r.quantity, unit_price: r.unit_price,
                discount_pct: r.discount_pct, order_amount: r.order_amount, payment_mode: r.payment_mode,
                order_status: r.order_status, channel: r.channel, delivery_minutes: r.delivery_minutes}),
       CAST(getvariable('now_utc') AS TIMESTAMP)
FROM stg_orders_batch s
JOIN raw_orders r ON r._run_id = s._run_id AND r._source_file = s._source_file AND r._source_line = s._source_line
WHERE s._run_id = getvariable('run_id') AND s.dq_status = 'FAIL'

UNION ALL
SELECT s._run_id, s._source_file, s._source_line, r.payment_id, s.failure_reason,
       to_json({payment_id: r.payment_id, order_id: r.order_id, payment_ts: r.payment_ts,
                payment_mode: r.payment_mode, amount_paid: r.amount_paid, payment_status: r.payment_status,
                gateway_ref: r.gateway_ref}),
       CAST(getvariable('now_utc') AS TIMESTAMP)
FROM stg_payments s
JOIN raw_payments r ON r._run_id = s._run_id AND r._source_line = s._source_line
WHERE s.dq_status = 'FAIL'

UNION ALL
SELECT s._run_id, s._source_file, s._source_line, r.customer_id, s.failure_reason,
       to_json({customer_id: r.customer_id, full_name: r.full_name, email: r.email, phone: r.phone, city: r.city,
                area: r.area, pincode: r.pincode, signup_date: r.signup_date, loyalty_tier: r.loyalty_tier,
                is_active: r.is_active}),
       CAST(getvariable('now_utc') AS TIMESTAMP)
FROM stg_customers s
JOIN raw_customers r ON r._run_id = s._run_id AND r._source_line = s._source_line
WHERE s.dq_status = 'FAIL'

UNION ALL
SELECT s._run_id, s._source_file, s._source_line, r.product_id, s.failure_reason,
       to_json({product_id: r.product_id, product_name: r.product_name, category: r.category,
                sub_category: r.sub_category, brand: r.brand, mrp: r.mrp, cost_price: r.cost_price, unit: r.unit,
                is_perishable: r.is_perishable, launch_date: r.launch_date}),
       CAST(getvariable('now_utc') AS TIMESTAMP)
FROM stg_products s
JOIN raw_products r ON r._run_id = s._run_id AND r._source_line = s._source_line
WHERE s.dq_status = 'FAIL'

UNION ALL
SELECT s._run_id, s._source_file, s._source_line, r.store_id, s.failure_reason,
       to_json({store_id: r.store_id, store_name: r.store_name, zone: r.zone, area: r.area, pincode: r.pincode,
                open_date: r.open_date, manager_name: r.manager_name,
                capacity_orders_per_day: r.capacity_orders_per_day}),
       CAST(getvariable('now_utc') AS TIMESTAMP)
FROM stg_stores s
JOIN raw_stores r ON r._run_id = s._run_id AND r._source_line = s._source_line
WHERE s.dq_status = 'FAIL';
