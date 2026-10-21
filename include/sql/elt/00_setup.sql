-- =============================================================================
-- 00_setup.sql : macros, raw layer and persistent staging tables for the ELT DAG
-- Run at the start of every nammamart_elt_athresh run (idempotent).
-- Business thresholds arrive as DuckDB variables set by the DAG from config.py,
-- so a rule change (e.g. the 120-minute delivery limit) is made in one place.
-- =============================================================================

-- ── Standardisation macros (mirror include/nammamart/etl_checks.py) ──────────
CREATE OR REPLACE MACRO nz(x) AS
    CASE WHEN x IS NULL OR upper(trim(x)) IN ('', 'N/A', 'NA', 'NULL', 'NONE', 'NAN', '-') THEN NULL ELSE trim(x) END;
CREATE OR REPLACE MACRO to_num(x) AS try_cast(nz(x) AS DOUBLE);
CREATE OR REPLACE MACRO to_ts(x) AS try_strptime(nz(x), '%Y-%m-%d %H:%M:%S');
CREATE OR REPLACE MACRO to_dt(x) AS CAST(try_strptime(nz(x), '%Y-%m-%d') AS DATE);
CREATE OR REPLACE MACRO capitalize_word(x) AS upper(left(x, 1)) || lower(substr(x, 2));
-- Known spellings of payment_mode in the exports (same map as config.PAYMENT_MODE_ALIASES)
CREATE OR REPLACE MACRO std_payment_mode(x) AS
    CASE lower(nz(x))
        WHEN 'upi' THEN 'UPI'
        WHEN 'card' THEN 'Card'
        WHEN 'cash on delivery' THEN 'Cash on Delivery'
        WHEN 'cod' THEN 'Cash on Delivery'
        WHEN 'cash' THEN 'Cash on Delivery'
        WHEN 'cashh' THEN 'Cash on Delivery'
        WHEN 'wallet' THEN 'Wallet'
    END;
-- Business date a file covers: orders_<date>_increment.csv -> <date>, base exports -> base_batch_date
CREATE OR REPLACE MACRO batch_date(f) AS
    coalesce(try_cast(regexp_extract(f, 'orders_(\d{4}-\d{2}-\d{2})_increment\.csv$', 1) AS DATE),
             CAST(getvariable('base_batch_date') AS DATE));
CREATE OR REPLACE MACRO batch_cutoff(f) AS CAST(batch_date(f) + INTERVAL 1 DAY AS TIMESTAMP);

-- ── PII masking (mirror include/nammamart/masking.py) ────────────────────────
CREATE OR REPLACE MACRO is_email(e) AS regexp_full_match(e, '[^@\s]+@[^@\s]+\.[^@\s]+');
CREATE OR REPLACE MACRO mask_email(e) AS
    CASE WHEN e IS NOT NULL AND is_email(e) THEN left(split_part(e, '@', 1), 2) || '***@' || split_part(e, '@', 2) END;
CREATE OR REPLACE MACRO digits_only(p) AS regexp_replace(coalesce(p, ''), '\D', '', 'g');
CREATE OR REPLACE MACRO mask_phone(p) AS
    CASE WHEN length(digits_only(p)) >= 4
         THEN left(digits_only(p), 2) || repeat('*', length(digits_only(p)) - 4) || right(digits_only(p), 2) END;
CREATE OR REPLACE MACRO mask_name(n) AS
    CASE WHEN nullif(trim(n), '') IS NULL THEN NULL
         WHEN len(regexp_split_to_array(trim(n), '\s+')) = 1 THEN trim(n)
         ELSE regexp_split_to_array(trim(n), '\s+')[1] || ' ' || left(regexp_split_to_array(trim(n), '\s+')[-1], 1) || '.'
    END;

-- ── Data-quality helpers ─────────────────────────────────────────────────────
-- failed_checks is a list of check names; turn it into the same text the ETL pipeline writes
CREATE OR REPLACE MACRO dq_reasons(checks, sev) AS (
    SELECT string_agg(check_name || ': ' || description, '; ' ORDER BY check_order)
    FROM dq_check_registry
    WHERE severity = sev AND list_contains(checks, check_name)
);
CREATE OR REPLACE MACRO dq_status_of(checks) AS
    CASE WHEN dq_reasons(checks, 'Critical') IS NULL THEN 'PASS' ELSE 'FAIL' END;

-- ── RAW layer: exact copy of every file, all text, append-only ───────────────
CREATE TABLE IF NOT EXISTS raw_orders (
    order_id VARCHAR, order_ts VARCHAR, customer_id VARCHAR, store_id VARCHAR, product_id VARCHAR,
    quantity VARCHAR, unit_price VARCHAR, discount_pct VARCHAR, order_amount VARCHAR, payment_mode VARCHAR,
    order_status VARCHAR, channel VARCHAR, delivery_minutes VARCHAR,
    _loaded_at TIMESTAMP, _source_file VARCHAR, _run_id VARCHAR, _source_line BIGINT
);
CREATE TABLE IF NOT EXISTS raw_payments (
    payment_id VARCHAR, order_id VARCHAR, payment_ts VARCHAR, payment_mode VARCHAR, amount_paid VARCHAR,
    payment_status VARCHAR, gateway_ref VARCHAR,
    _loaded_at TIMESTAMP, _source_file VARCHAR, _run_id VARCHAR, _source_line BIGINT
);
CREATE TABLE IF NOT EXISTS raw_customers (
    customer_id VARCHAR, full_name VARCHAR, email VARCHAR, phone VARCHAR, city VARCHAR, area VARCHAR,
    pincode VARCHAR, signup_date VARCHAR, loyalty_tier VARCHAR, is_active VARCHAR,
    _loaded_at TIMESTAMP, _source_file VARCHAR, _run_id VARCHAR, _source_line BIGINT
);
CREATE TABLE IF NOT EXISTS raw_products (
    product_id VARCHAR, product_name VARCHAR, category VARCHAR, sub_category VARCHAR, brand VARCHAR,
    mrp VARCHAR, cost_price VARCHAR, unit VARCHAR, is_perishable VARCHAR, launch_date VARCHAR,
    _loaded_at TIMESTAMP, _source_file VARCHAR, _run_id VARCHAR, _source_line BIGINT
);
CREATE TABLE IF NOT EXISTS raw_stores (
    store_id VARCHAR, store_name VARCHAR, zone VARCHAR, area VARCHAR, pincode VARCHAR, open_date VARCHAR,
    manager_name VARCHAR, capacity_orders_per_day VARCHAR,
    _loaded_at TIMESTAMP, _source_file VARCHAR, _run_id VARCHAR, _source_line BIGINT
);

-- ── STAGING: orders are kept across runs and MERGEd on order_id ──────────────
-- stg_orders_batch : every order row evaluated by a run (including duplicate copies)
-- stg_orders       : current version of each order_id, PASS or FAIL
CREATE TABLE IF NOT EXISTS stg_orders_batch (
    order_id VARCHAR, order_ts TIMESTAMP, order_date DATE, customer_id VARCHAR, product_id VARCHAR,
    store_id VARCHAR, quantity INTEGER, unit_price DOUBLE, discount_pct DOUBLE, order_amount DOUBLE,
    payment_mode VARCHAR, order_status VARCHAR, channel VARCHAR, delivery_minutes INTEGER,
    dq_status VARCHAR, failure_reason VARCHAR, dq_warnings VARCHAR, failed_checks VARCHAR[], is_duplicate BOOLEAN,
    _batch_date DATE, _source_file VARCHAR, _source_line BIGINT, _run_id VARCHAR, _evaluated_at TIMESTAMP
);
CREATE TABLE IF NOT EXISTS stg_orders (
    order_id VARCHAR PRIMARY KEY, order_ts TIMESTAMP, order_date DATE, customer_id VARCHAR, product_id VARCHAR,
    store_id VARCHAR, quantity INTEGER, unit_price DOUBLE, discount_pct DOUBLE, order_amount DOUBLE,
    payment_mode VARCHAR, order_status VARCHAR, channel VARCHAR, delivery_minutes INTEGER,
    dq_status VARCHAR, failure_reason VARCHAR, dq_warnings VARCHAR, failed_checks VARCHAR[],
    _batch_date DATE, _source_file VARCHAR, _source_line BIGINT, _run_id VARCHAR, _merged_at TIMESTAMP
);
CREATE TABLE IF NOT EXISTS elt_merge_log (
    run_id VARCHAR, source_file VARCHAR, rows_in_batch BIGINT, inserted BIGINT, updated BIGINT, changed BIGINT,
    skipped BIGINT, pass_rows_written BIGINT, merged_at TIMESTAMP
);
