-- =============================================================================
-- stg_customers.sql : this run's raw_customers -> typed, standardised, PII masked
-- Keeps every row with dq_status / failure_reason; only duplicate or missing
-- customer_id is Critical (FAIL). Raw email / phone never leave this statement.
-- =============================================================================
CREATE OR REPLACE TABLE stg_customers AS
WITH typed AS (
    SELECT
        nz(customer_id) AS customer_id, nz(full_name) AS full_name, nz(email) AS email, nz(phone) AS phone,
        nz(city) AS city, nz(area) AS area, nz(pincode) AS pincode, to_dt(signup_date) AS signup_date,
        capitalize_word(nz(loyalty_tier)) AS loyalty_tier, loyalty_tier AS loyalty_tier_src,
        CASE upper(nz(is_active)) WHEN 'Y' THEN true WHEN 'N' THEN false END AS is_active,
        _source_file, _source_line, _run_id
    FROM raw_customers
    WHERE _run_id = getvariable('run_id')
),
emails AS (
    SELECT lower(email) AS email_key, count(DISTINCT customer_id) AS ids_per_email
    FROM typed WHERE email IS NOT NULL GROUP BY 1
),
flagged AS (
    SELECT t.*,
        list_filter([
            CASE WHEN t.customer_id IS NULL THEN 'customers_customer_id_not_null' END,
            CASE WHEN t.city IS NULL THEN 'customers_city_present' END,
            CASE WHEN t.customer_id IS NOT NULL AND row_number() OVER (PARTITION BY t.customer_id ORDER BY t._source_line) > 1
                 THEN 'customers_customer_id_unique' END,
            CASE WHEN e.ids_per_email > 1 THEN 'customers_email_unique' END,
            CASE WHEN t.email IS NULL OR NOT is_email(t.email) THEN 'customers_email_format' END,
            CASE WHEN t.phone IS NULL OR NOT regexp_full_match(t.phone, '\d{10}') THEN 'customers_phone_format' END,
            CASE WHEN t.pincode IS NULL OR NOT regexp_full_match(t.pincode, '\d{6}') THEN 'customers_pincode_format' END,
            CASE WHEN t.loyalty_tier IS NULL OR t.loyalty_tier NOT IN ('Bronze', 'Silver', 'Gold', 'Platinum')
                 THEN 'customers_loyalty_tier_valid' END,
            CASE WHEN t.loyalty_tier IN ('Bronze', 'Silver', 'Gold', 'Platinum') AND t.loyalty_tier_src <> t.loyalty_tier
                 THEN 'customers_loyalty_tier_casing' END,
            CASE WHEN t.signup_date >= batch_cutoff(t._source_file) THEN 'customers_signup_not_future' END
        ], lambda x: x IS NOT NULL) AS failed_checks
    FROM typed t LEFT JOIN emails e ON e.email_key = lower(t.email)
)
SELECT
    customer_id,
    mask_name(full_name) AS customer_name_masked,
    mask_email(email) AS email_masked,
    mask_phone(phone) AS phone_masked,
    city, area, pincode, signup_date,
    CASE WHEN loyalty_tier IN ('Bronze', 'Silver', 'Gold', 'Platinum') THEN loyalty_tier ELSE 'Unknown' END AS loyalty_tier,
    is_active,
    dq_status_of(failed_checks) AS dq_status,
    dq_reasons(failed_checks, 'Critical') AS failure_reason,
    dq_reasons(failed_checks, 'Warning') AS dq_warnings,
    failed_checks, _source_file, _source_line, _run_id
FROM flagged
ORDER BY _source_line;
