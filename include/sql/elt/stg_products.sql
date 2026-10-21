-- =============================================================================
-- stg_products.sql : this run's raw_products -> typed and standardised
-- When a product_id appears twice the version labelled '(old)' is the duplicate.
-- =============================================================================
CREATE OR REPLACE TABLE stg_products AS
WITH typed AS (
    SELECT
        nz(product_id) AS product_id, nz(product_name) AS product_name, nz(category) AS category,
        nz(sub_category) AS sub_category, nz(brand) AS brand, to_num(mrp) AS mrp, to_num(cost_price) AS cost_price,
        nz(unit) AS unit,
        CASE lower(nz(is_perishable)) WHEN 'true' THEN true WHEN 'false' THEN false END AS is_perishable,
        to_dt(launch_date) AS launch_date,
        _source_file, _source_line, _run_id
    FROM raw_products
    WHERE _run_id = getvariable('run_id')
),
ranked AS (
    SELECT *, row_number() OVER (
        PARTITION BY product_id
        ORDER BY coalesce(product_name ILIKE '%(old)%', false), _source_line) AS rn
    FROM typed
),
flagged AS (
    SELECT *,
        list_filter([
            CASE WHEN product_id IS NULL THEN 'products_product_id_not_null' END,
            CASE WHEN category IS NULL THEN 'products_category_present' END,
            CASE WHEN product_id IS NOT NULL AND rn > 1 THEN 'products_product_id_unique' END,
            CASE WHEN category IS NOT NULL AND category NOT IN ('Fruits & Vegetables', 'Dairy & Bakery', 'Staples',
                 'Snacks & Beverages', 'Personal Care', 'Household') THEN 'products_category_valid' END,
            CASE WHEN mrp IS NULL OR mrp <= 0 THEN 'products_mrp_positive' END,
            CASE WHEN cost_price > mrp THEN 'products_cost_within_mrp' END,
            CASE WHEN launch_date >= batch_cutoff(_source_file) THEN 'products_launch_not_future' END
        ], lambda x: x IS NOT NULL) AS failed_checks
    FROM ranked
)
SELECT
    product_id, product_name, coalesce(category, 'Unknown') AS category, sub_category, brand, mrp, cost_price, unit,
    is_perishable, launch_date,
    dq_status_of(failed_checks) AS dq_status,
    dq_reasons(failed_checks, 'Critical') AS failure_reason,
    dq_reasons(failed_checks, 'Warning') AS dq_warnings,
    failed_checks, _source_file, _source_line, _run_id
FROM flagged
ORDER BY _source_line;
