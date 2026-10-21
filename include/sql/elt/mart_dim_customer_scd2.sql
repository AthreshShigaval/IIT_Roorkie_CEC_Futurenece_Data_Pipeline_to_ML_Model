-- =============================================================================
-- mart_dim_customer_scd2.sql : SCD Type 2 customer dimension (bonus).
-- Keeps every loyalty_tier / is_active version a customer has had:
--   a change closes the current row (valid_to, is_current = false) and opens a new one.
-- Masked attributes only. Business date of the change = the customers export date.
-- =============================================================================
CREATE TABLE IF NOT EXISTS dim_customer_scd2 (
    customer_sk VARCHAR PRIMARY KEY,          -- customer_id + version start, stable surrogate key
    customer_id VARCHAR, customer_name_masked VARCHAR, email_masked VARCHAR, phone_masked VARCHAR,
    area VARCHAR, loyalty_tier VARCHAR, is_active BOOLEAN,
    valid_from TIMESTAMP, valid_to TIMESTAMP, is_current BOOLEAN, _run_id VARCHAR
);

CREATE OR REPLACE TEMP VIEW scd_src AS
SELECT customer_id, customer_name_masked, email_masked, phone_masked, area, loyalty_tier, is_active,
       CAST(getvariable('now_utc') AS TIMESTAMP) AS change_ts
FROM stg_customers WHERE dq_status = 'PASS';

-- 1. close the current version when a tracked attribute changed
UPDATE dim_customer_scd2 d
SET valid_to = s.change_ts, is_current = false
FROM scd_src s
WHERE d.customer_id = s.customer_id AND d.is_current
  AND (d.loyalty_tier IS DISTINCT FROM s.loyalty_tier OR d.is_active IS DISTINCT FROM s.is_active);

-- 2. open a version for new customers and for the ones just closed
INSERT INTO dim_customer_scd2
SELECT s.customer_id || '@' || strftime(s.change_ts, '%Y%m%dT%H%M%S'), s.customer_id, s.customer_name_masked,
       s.email_masked, s.phone_masked, s.area, s.loyalty_tier, s.is_active,
       s.change_ts, TIMESTAMP '9999-12-31 00:00:00', true, getvariable('run_id')
FROM scd_src s
WHERE NOT EXISTS (SELECT 1 FROM dim_customer_scd2 d WHERE d.customer_id = s.customer_id AND d.is_current);
