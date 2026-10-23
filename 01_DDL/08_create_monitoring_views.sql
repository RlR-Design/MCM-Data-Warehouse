/*
===============================================================================
Script   : 08_create_monitoring_views.sql
Layer    : Monitoring
Purpose  : Bird's-eye view of scrape runs, combining the scraper's run summary
           with the warehouse's scrape health checks:
             mon_scrape_shop_runs  one row per shop per run
             mon_scrape_runs       one row per run, with an overall status
           Intended for engineers and as the source of a monitoring page.
When     : Initial setup, after 07_create_gold_market_views.sql.
           Re-run after any change to this file. Safe to re-run.
Warning  : Views hold no data. Runs appear after sp_check_scrape_health has run.
===============================================================================
*/

-- -----------------------------------------------------------------------------
-- mon_scrape_shop_runs: one row per shop per run (Bronze batch)
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW statement_competitor_data_warehouse.mon_scrape_shop_runs
OPTIONS(description="One row per shop per scrape run: scraper report, warehouse health result and counts")
AS
-- Scraper's run summary, typed (latest row per shop per batch).
WITH run_summary AS (
    SELECT
        batch_id,
        TRIM(raw_shop_name) AS shop_name,
        NULLIF(TRIM(raw_run_id), '') AS run_id,
        SAFE_CAST(TRIM(raw_started_at) AS TIMESTAMP) AS started_at,
        SAFE_CAST(TRIM(raw_finished_at) AS TIMESTAMP) AS finished_at,
        SAFE_CAST(TRIM(raw_pages_scraped) AS INT64) AS pages_scraped,
        SAFE_CAST(TRIM(raw_http_errors) AS INT64) AS http_errors,
        SAFE_CAST(TRIM(raw_retries) AS INT64) AS retries,
        NULLIF(TRIM(raw_error_message), '') AS error_message,
        NULLIF(TRIM(raw_scraper_version), '') AS scraper_version
    FROM statement_competitor_data_warehouse.bronze_scrape_run_summary
    WHERE NULLIF(TRIM(raw_shop_name), '') IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY batch_id, TRIM(raw_shop_name) ORDER BY ingested_at DESC) = 1
),
-- Load time per batch.
batches AS (
    SELECT batch_id, MIN(ingested_at) AS loaded_at
    FROM statement_competitor_data_warehouse.bronze_raw_products
    GROUP BY batch_id
)
SELECT
    h.batch_id,
    b.loaded_at,
    rs.run_id,
    h.shop_name,
    h.status AS health_status,
    NULLIF(h.issues, '') AS issues,
    h.scraper_status,
    rs.started_at,
    rs.finished_at,
    ROUND(TIMESTAMP_DIFF(rs.finished_at, rs.started_at, SECOND) / 60, 1) AS duration_minutes,
    rs.pages_scraped,
    h.products_expected,
    h.products_found,
    h.product_count AS products_in_csv,
    h.baseline_count,
    ROUND(SAFE_DIVIDE(h.product_count, h.baseline_count) * 100, 1) AS pct_of_baseline,
    ROUND(SAFE_DIVIDE(h.product_count, h.products_expected) * 100, 1) AS pct_of_site,
    ROUND(h.quarantine_rate * 100, 1) AS pct_quarantined,
    rs.http_errors,
    rs.retries,
    rs.error_message,
    rs.scraper_version,
    h.checked_at
FROM statement_competitor_data_warehouse.bronze_scrape_health h
LEFT JOIN run_summary rs ON rs.batch_id = h.batch_id AND rs.shop_name = h.shop_name
LEFT JOIN batches b ON b.batch_id = h.batch_id;

-- -----------------------------------------------------------------------------
-- mon_scrape_runs: one row per run with an overall status
--   FAILED   any shop FAILED
--   WARNING  any shop WARNING
--   HEALTHY  all shops HEALTHY
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW statement_competitor_data_warehouse.mon_scrape_runs
OPTIONS(description="One row per scrape run: overall status, shop counts per status, product totals and duration")
AS
SELECT
    batch_id,
    MIN(loaded_at) AS loaded_at,
    ANY_VALUE(run_id) AS run_id,
    CASE
        WHEN COUNTIF(health_status = 'FAILED') > 0 THEN 'FAILED'
        WHEN COUNTIF(health_status = 'WARNING') > 0 THEN 'WARNING'
        ELSE 'HEALTHY'
    END AS run_status,
    COUNT(*) AS shops,
    COUNTIF(health_status = 'HEALTHY') AS shops_healthy,
    COUNTIF(health_status = 'WARNING') AS shops_warning,
    COUNTIF(health_status = 'FAILED') AS shops_failed,
    STRING_AGG(IF(health_status = 'FAILED', FORMAT('%s (%s)', shop_name, issues), NULL), ', ' ORDER BY shop_name) AS failed_shops,
    STRING_AGG(IF(health_status = 'WARNING', FORMAT('%s (%s)', shop_name, issues), NULL), ', ' ORDER BY shop_name) AS warning_shops,
    SUM(products_expected) AS products_expected,
    SUM(products_found) AS products_found,
    SUM(products_in_csv) AS products_in_csv,
    MIN(started_at) AS started_at,
    MAX(finished_at) AS finished_at,
    ROUND(TIMESTAMP_DIFF(MAX(finished_at), MIN(started_at), SECOND) / 60, 1) AS duration_minutes,
    SUM(http_errors) AS http_errors,
    LOGICAL_OR(scraper_status IS NOT NULL) AS has_run_summary
FROM statement_competitor_data_warehouse.mon_scrape_shop_runs
GROUP BY batch_id;


/*
===============================================================================
Tests (run manually)
===============================================================================
-- Both monitoring views exist (expect table_type = VIEW).
SELECT table_name, table_type
FROM `statement_competitor_data_warehouse.INFORMATION_SCHEMA.TABLES`
WHERE table_name IN ('mon_scrape_shop_runs', 'mon_scrape_runs')
ORDER BY table_name;

-- Recent runs, newest first.
SELECT loaded_at, run_status, shops, shops_healthy, shops_warning, shops_failed,
       products_in_csv, products_found, products_expected, duration_minutes, failed_shops
FROM statement_competitor_data_warehouse.mon_scrape_runs
ORDER BY loaded_at DESC
LIMIT 20;

-- Shop detail for the latest run.
SELECT shop_name, health_status, issues, scraper_status, products_in_csv, products_found,
       products_expected, pct_of_baseline, pct_of_site, duration_minutes, error_message
FROM statement_competitor_data_warehouse.mon_scrape_shop_runs
WHERE batch_id = (SELECT batch_id FROM statement_competitor_data_warehouse.mon_scrape_runs ORDER BY loaded_at DESC LIMIT 1)
ORDER BY health_status, shop_name;

-- Shops that fail most often.
SELECT shop_name, COUNTIF(health_status = 'FAILED') AS failed_runs, COUNT(*) AS runs,
       ROUND(AVG(duration_minutes), 1) AS avg_minutes
FROM statement_competitor_data_warehouse.mon_scrape_shop_runs
GROUP BY shop_name
ORDER BY failed_runs DESC;
*/
