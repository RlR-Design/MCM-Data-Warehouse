/*
===============================================================================
Script   : pipeline_tests.sql
Layer    : All
Purpose  : Every check used to verify the V2 pipeline, in the order they are
           run: setup, after each pipeline run, lifecycle (Gold) and automation.
           Read-only: nothing here changes data.
When     : After setup, after each run, and when investigating an issue.
           Run one query at a time (cursor inside it, Ctrl/Cmd+Enter).
Expected : "Expect" comments give the rule that must hold. "Test data" gives the
           result of the verification run of 2026-10-24:
             week 1 = MCM_All_products_2026-09-15.csv (real scrape)
             week 2 = MCM_All_products_2026-09-22.csv (synthetic, 21 known changes)
           The per-change test for week 2 (real URLs) is kept locally in
           06_Datasets/week2_expected_results.sql.
===============================================================================
*/


-- =============================================================================
-- 1. Setup
-- =============================================================================

-- 1.1 Dataset exists: fails if it does not; returns 0 while it is empty.
SELECT COUNT(*) AS object_count
FROM `statement_competitor_data_warehouse.INFORMATION_SCHEMA.TABLES`;

-- 1.2 Objects per type.
-- Expect after 01_DDL/01-09: BASE TABLE 22, VIEW 19.
SELECT table_type, COUNT(*) AS objects
FROM `statement_competitor_data_warehouse.INFORMATION_SCHEMA.TABLES`
GROUP BY table_type;

-- 1.3 Procedures deployed.
-- Expect 10: sp_check_scrape_health ... sp_validate_silver_products, sp_run_pipeline, sp_set_shop_rule.
SELECT routine_name, created, last_altered
FROM `statement_competitor_data_warehouse.INFORMATION_SCHEMA.ROUTINES`
ORDER BY routine_name;

-- 1.4 Shop rules in effect (shop value, else '*' default).
-- Expect: '*' row present; Statement Collectables max_quarantine_rate 0.3.
SELECT
    r.shop_name,
    COALESCE(r.zero_price_means_sold, d.zero_price_means_sold) AS zero_price_means_sold,
    COALESCE(r.delist_grace_scrapes, d.delist_grace_scrapes) AS delist_grace_scrapes,
    COALESCE(r.min_count_ratio, d.min_count_ratio) AS min_count_ratio,
    COALESCE(r.max_quarantine_rate, d.max_quarantine_rate) AS max_quarantine_rate,
    COALESCE(r.max_duplicate_rate, d.max_duplicate_rate) AS max_duplicate_rate,
    COALESCE(r.max_field_empty_rate, d.max_field_empty_rate) AS max_field_empty_rate,
    COALESCE(r.min_expected_ratio, d.min_expected_ratio) AS min_expected_ratio,
    r.notes
FROM statement_competitor_data_warehouse.ref_shop_rules r
CROSS JOIN (SELECT * FROM statement_competitor_data_warehouse.ref_shop_rules WHERE shop_name = '*') d
ORDER BY r.shop_name;

-- 1.5 Shop locations seeded. Expect one row per known shop, no duplicates.
SELECT LOWER(shop_name) AS shop, COUNT(*) AS rows_per_shop
FROM statement_competitor_data_warehouse.ref_shop_locations
GROUP BY 1
ORDER BY 1;


-- =============================================================================
-- 2. After every pipeline run
-- =============================================================================

-- 2.1 Every step completed.
-- Expect INFO_STAGE_COMPLETE for all 8 steps; step 8 reads "0 of 10 checks found issues".
SELECT 'bronze' AS log, scraper_job_id AS step, error_type, error_details, error_timestamp
FROM statement_competitor_data_warehouse.bronze_raw_error_logs
QUALIFY ROW_NUMBER() OVER (PARTITION BY scraper_job_id ORDER BY error_timestamp DESC) = 1
UNION ALL
SELECT 'silver', scraper_job_id, error_type, error_details, error_timestamp
FROM statement_competitor_data_warehouse.silver_transformed_error_logs
QUALIFY ROW_NUMBER() OVER (PARTITION BY scraper_job_id ORDER BY error_timestamp DESC) = 1
UNION ALL
SELECT 'normalised', procedure_name, error_type, error_details, error_timestamp
FROM statement_competitor_data_warehouse.silver_normalised_error_logs
QUALIFY ROW_NUMBER() OVER (PARTITION BY procedure_name ORDER BY error_timestamp DESC) = 1
UNION ALL
SELECT 'historisation', procedure_name, error_type, error_details, error_timestamp
FROM statement_competitor_data_warehouse.silver_historisation_error_logs
QUALIFY ROW_NUMBER() OVER (PARTITION BY procedure_name ORDER BY error_timestamp DESC) = 1
UNION ALL
SELECT 'reconciled', procedure_name, error_type, error_details, error_timestamp
FROM statement_competitor_data_warehouse.silver_reconciled_error_logs
QUALIFY ROW_NUMBER() OVER (PARTITION BY procedure_name ORDER BY error_timestamp DESC) = 1
ORDER BY error_timestamp;

-- 2.2 Errors and warnings in the last 7 days. Expect no rows (RUN_SUMMARY_MISSING is
-- expected for a CSV without a run summary).
SELECT 'bronze' AS log, error_timestamp, scraper_job_id AS step, error_type, error_details
FROM statement_competitor_data_warehouse.bronze_raw_error_logs
WHERE error_type NOT LIKE 'INFO%' AND error_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
UNION ALL
SELECT 'silver', error_timestamp, scraper_job_id, error_type, error_details
FROM statement_competitor_data_warehouse.silver_transformed_error_logs
WHERE error_type NOT LIKE 'INFO%' AND error_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
UNION ALL
SELECT 'normalised', error_timestamp, procedure_name, error_type, error_details
FROM statement_competitor_data_warehouse.silver_normalised_error_logs
WHERE error_type NOT LIKE 'INFO%' AND error_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
UNION ALL
SELECT 'historisation', error_timestamp, procedure_name, error_type, error_details
FROM statement_competitor_data_warehouse.silver_historisation_error_logs
WHERE error_type NOT LIKE 'INFO%' AND error_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
UNION ALL
SELECT 'reconciled', error_timestamp, procedure_name, error_type, error_details
FROM statement_competitor_data_warehouse.silver_reconciled_error_logs
WHERE error_type NOT LIKE 'INFO%' AND error_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
ORDER BY error_timestamp DESC;

-- 2.3 Run health, one row per run. Expect run_status HEALTHY.
-- Test data: week 1 HEALTHY, 9/9 shops, products_in_csv 583, has_run_summary FALSE;
--            week 2 HEALTHY, 9/9 shops, products_found 584, products_in_csv 584, has_run_summary TRUE.
SELECT batch_id, loaded_at, run_status, shops, shops_healthy, shops_warning, shops_failed,
       failed_shops, warning_shops, products_expected, products_found, products_in_csv, has_run_summary
FROM statement_competitor_data_warehouse.mon_scrape_runs
ORDER BY loaded_at;

-- 2.4 Shops that were not healthy, with the measured values. Expect no rows.
SELECT batch_id, shop_name, status, issues, product_count, baseline_count,
       ROUND(quarantine_rate * 100, 1) AS pct_quarantined, details
FROM statement_competitor_data_warehouse.bronze_scrape_health
WHERE status != 'HEALTHY'
ORDER BY checked_at DESC;

-- 2.5 Rows loaded and quarantined in Bronze per batch.
-- Expect each CSV loaded once (one batch per file).
-- Test data: week 1 584 rows / 19 quarantined; week 2 585 / 20.
SELECT
    b.batch_id,
    MIN(b.ingested_at) AS loaded_at,
    COUNT(*) AS rows_loaded,
    (SELECT COUNT(*) FROM statement_competitor_data_warehouse.bronze_quarantine_bad_records q
     WHERE q.batch_id = b.batch_id) AS bronze_quarantined
FROM statement_competitor_data_warehouse.bronze_raw_products b
GROUP BY b.batch_id
ORDER BY loaded_at;

-- 2.6 Quarantine reasons per layer.
-- Test data: bronze 'Price not parseable' 39 (19 + 20); silver PRICE_OUT_OF_RANGE 6 (3 + 3).
SELECT 'bronze' AS layer, error_reason, COUNT(*) AS records, ANY_VALUE(raw_payload) AS example
FROM statement_competitor_data_warehouse.bronze_quarantine_bad_records
GROUP BY error_reason
UNION ALL
SELECT 'silver', error_reason, COUNT(*), ANY_VALUE(raw_payload)
FROM statement_competitor_data_warehouse.silver_quarantine_bad_records
GROUP BY error_reason
ORDER BY layer, records DESC;

-- 2.7 Quarantined Bronze rows per shop in the latest batch (price format problems show here).
SELECT
    JSON_VALUE(raw_payload, '$.raw_shop_name') AS shop_name,
    error_reason,
    COUNT(*) AS records,
    ARRAY_AGG(JSON_VALUE(raw_payload, '$.raw_price') IGNORE NULLS LIMIT 5) AS example_prices
FROM statement_competitor_data_warehouse.bronze_quarantine_bad_records
WHERE batch_id = (SELECT MAX(batch_id) FROM statement_competitor_data_warehouse.bronze_raw_products)
GROUP BY 1, 2
ORDER BY records DESC;

-- 2.8 Silver products and scrape time parsing. Expect no_scrape_time = 0.
-- Test data: week 1 561 products; week 2 565.
SELECT COUNT(*) AS products, COUNTIF(timestamp_date IS NULL) AS no_scrape_time
FROM statement_competitor_data_warehouse.silver_transformed_products;

-- 2.9 Only changed rows are rewritten: products per scrape date.
-- Test data after week 2: 2026-09-15 549, 2026-09-22 16 (12 changed + 4 new).
SELECT timestamp_date, COUNT(*) AS products
FROM statement_competitor_data_warehouse.silver_transformed_products
GROUP BY timestamp_date
ORDER BY timestamp_date;

-- 2.10 Star schema in line with Silver. Expect fact_rows = silver_products and
-- orphan_keys = 0. Test data: 9 shops.
SELECT
    (SELECT COUNT(*) FROM statement_competitor_data_warehouse.silver_transformed_products) AS silver_products,
    (SELECT COUNT(*) FROM statement_competitor_data_warehouse.fact_product_scrape) AS fact_rows,
    (SELECT COUNT(*) FROM statement_competitor_data_warehouse.dim_shop) AS shops,
    (SELECT COUNT(*) FROM statement_competitor_data_warehouse.dim_category) AS categories,
    (SELECT COUNT(*) FROM statement_competitor_data_warehouse.dim_maker) AS makers,
    (SELECT COUNT(*) FROM statement_competitor_data_warehouse.dim_material) AS materials,
    (SELECT COUNT(*)
     FROM statement_competitor_data_warehouse.fact_product_scrape f
     LEFT JOIN statement_competitor_data_warehouse.dim_shop s USING (shop_id)
     LEFT JOIN statement_competitor_data_warehouse.dim_category c USING (category_id)
     LEFT JOIN statement_competitor_data_warehouse.dim_maker m USING (maker_id)
     LEFT JOIN statement_competitor_data_warehouse.dim_material mt USING (material_id)
     WHERE s.shop_id IS NULL OR c.category_id IS NULL OR m.maker_id IS NULL OR mt.material_id IS NULL) AS orphan_keys;

-- 2.11 Price history (SCD Type 2). Expect current_versions = Silver products.
-- Test data: week 1 561 versions / 561 current; week 2 577 / 565 current / 12 superseded,
-- all superseded versions ending on one date (distinct_end_dates = 1).
SELECT
    COUNT(*) AS versions,
    COUNTIF(is_current) AS current_versions,
    COUNTIF(NOT is_current) AS superseded,
    COUNT(DISTINCT IF(NOT is_current, DATE(valid_to), NULL)) AS distinct_end_dates
FROM statement_competitor_data_warehouse.dim_price_history;

-- 2.12 Price history integrity. Expect every count 0.
SELECT
    (SELECT COUNT(*) FROM (
        SELECT product_id FROM statement_competitor_data_warehouse.dim_price_history
        WHERE is_current GROUP BY product_id HAVING COUNT(*) > 1)) AS duplicate_current_versions,
    (SELECT COUNT(*) FROM statement_competitor_data_warehouse.dim_price_history
     WHERE valid_to < valid_from) AS invalid_intervals,
    (SELECT COUNT(*) FROM (
        SELECT valid_from, LAG(valid_to) OVER (PARTITION BY product_id ORDER BY valid_from) AS prev_valid_to
        FROM statement_competitor_data_warehouse.dim_price_history)
     WHERE prev_valid_to IS NOT NULL AND prev_valid_to != valid_from) AS gaps_or_overlaps;


-- =============================================================================
-- 3. Lifecycle and reporting (Gold)
-- =============================================================================

-- 3.1 Events per type.
-- Test data after week 2: NEW_LISTING 588, MARKDOWN 5, PRICE_INCREASE 2,
-- SOLD_MARKED 3, SOLD_ZERO_PRICE 2; no DELISTED, RELISTED or SCRAPE_GAP.
SELECT event_type, COUNT(*) AS events
FROM statement_competitor_data_warehouse.gold_product_events
GROUP BY event_type
ORDER BY event_type;

-- 3.2 Status per product.
-- Test data after week 2 (before the QUARANTINED status): ACTIVE 551, ACTIVE_DISCOUNTED 5,
-- PENDING_DELIST 4, SOLD/MARKED_SOLD 3, SOLD/ZERO_PRICE 2 (565). With QUARANTINED: + 23.
SELECT status, sold_reason, COUNT(*) AS products
FROM statement_competitor_data_warehouse.gold_product_status
GROUP BY status, sold_reason
ORDER BY status, sold_reason;

-- 3.3 One status row per product. Expect no rows.
SELECT product_id, COUNT(*) AS rows_per_product
FROM statement_competitor_data_warehouse.gold_product_status
GROUP BY product_id
HAVING COUNT(*) > 1;

-- 3.4 Quarantined products and why.
-- Test data: week 1 22 (19 'Price not parseable', 3 PRICE_OUT_OF_RANGE); week 2 23 (+ the POA product).
SELECT shop_name, quarantine_reason, COUNT(*) AS products
FROM statement_competitor_data_warehouse.gold_product_status
WHERE status = 'QUARANTINED'
GROUP BY shop_name, quarantine_reason
ORDER BY products DESC;

-- 3.5 Sales. Expect sold_price never 0 or NULL.
-- Test data after week 2: 5 rows, days_on_market 7; MARKED_SOLD at the listed price,
-- ZERO_PRICE at the price before R0.00.
SELECT shop_name, title, sold_reason, sold_at, sold_price, first_listed_price, days_on_market
FROM statement_competitor_data_warehouse.gold_sales
ORDER BY sold_reason, shop_name;

-- 3.6 Market base view covers every validated product. Expect equal counts.
SELECT
    (SELECT COUNT(*) FROM statement_competitor_data_warehouse.gold_market_products) AS market_products,
    (SELECT COUNTIF(status != 'QUARANTINED') FROM statement_competitor_data_warehouse.gold_product_status) AS validated_products;

-- 3.7 Sales reconcile across views. Expect three equal counts.
SELECT
    (SELECT COUNT(*) FROM statement_competitor_data_warehouse.gold_sales) AS gold_sales,
    (SELECT SUM(sales) FROM statement_competitor_data_warehouse.gold_regional_demand) AS regional_sales,
    (SELECT SUM(sales) FROM statement_competitor_data_warehouse.gold_shop_scorecard) AS shop_sales;


-- =============================================================================
-- 4. Automation
-- =============================================================================

-- 4.1 Uploads handled by the trigger function. Expect one row per uploaded CSV.
SELECT gcs_uri, object_generation, triggered_at
FROM statement_competitor_data_warehouse.ops_pipeline_trigger_log
ORDER BY triggered_at DESC
LIMIT 20;

-- 4.2 Every triggered file reached Bronze. Expect loaded = TRUE for each.
SELECT
    t.gcs_uri,
    t.triggered_at,
    EXISTS (
        SELECT 1 FROM statement_competitor_data_warehouse.bronze_raw_error_logs l
        WHERE l.scraper_job_id = 'sp_load_bronze_products'
          AND l.error_type = 'INFO_STAGE_COMPLETE'
          AND l.error_timestamp >= t.triggered_at
    ) AS loaded
FROM statement_competitor_data_warehouse.ops_pipeline_trigger_log t
ORDER BY t.triggered_at DESC;


-- =============================================================================
-- 5. Reset (01_DDL/00_reset_dataset.sql)
-- =============================================================================

-- 5.1 Objects left after a reset. Expect no rows from either query.
SELECT table_type, COUNT(*) AS objects
FROM `statement_competitor_data_warehouse.INFORMATION_SCHEMA.TABLES`
GROUP BY table_type;

SELECT routine_type, COUNT(*) AS objects
FROM `statement_competitor_data_warehouse.INFORMATION_SCHEMA.ROUTINES`
GROUP BY routine_type;
