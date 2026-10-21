-- =============================================================================
-- stg_stores.sql : this run's raw_stores -> typed and standardised
-- =============================================================================
CREATE OR REPLACE TABLE stg_stores AS
WITH typed AS (
    SELECT
        nz(store_id) AS store_id, nz(store_name) AS store_name, nz(zone) AS zone, nz(area) AS area,
        nz(pincode) AS pincode, to_dt(open_date) AS open_date, nz(manager_name) AS manager_name,
        to_num(capacity_orders_per_day) AS capacity_orders_per_day,
        _source_file, _source_line, _run_id
    FROM raw_stores
    WHERE _run_id = getvariable('run_id')
),
flagged AS (
    SELECT *,
        list_filter([
            CASE WHEN store_id IS NULL THEN 'stores_store_id_not_null' END,
            CASE WHEN zone IS NULL THEN 'stores_zone_present' END,
            CASE WHEN store_id IS NOT NULL AND row_number() OVER (PARTITION BY store_id ORDER BY _source_line) > 1
                 THEN 'stores_store_id_unique' END,
            CASE WHEN zone IS NOT NULL AND zone NOT IN ('North', 'South', 'East', 'West') THEN 'stores_zone_valid' END,
            CASE WHEN pincode IS NULL OR NOT regexp_full_match(pincode, '\d{6}') THEN 'stores_pincode_format' END,
            CASE WHEN capacity_orders_per_day IS NULL OR capacity_orders_per_day <= 0 THEN 'stores_capacity_positive' END,
            CASE WHEN open_date >= batch_cutoff(_source_file) THEN 'stores_open_not_future' END
        ], lambda x: x IS NOT NULL) AS failed_checks
    FROM typed
)
SELECT
    store_id, store_name, coalesce(zone, 'Unknown') AS zone, area, pincode, open_date, manager_name,
    try_cast(capacity_orders_per_day AS INTEGER) AS capacity_orders_per_day,
    dq_status_of(failed_checks) AS dq_status,
    dq_reasons(failed_checks, 'Critical') AS failure_reason,
    dq_reasons(failed_checks, 'Warning') AS dq_warnings,
    failed_checks, _source_file, _source_line, _run_id
FROM flagged
ORDER BY _source_line;
