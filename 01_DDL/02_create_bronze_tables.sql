/*
===============================================================================
Script   : 02_create_bronze_tables.sql
Layer    : Bronze
Purpose  : Creates the Bronze landing tables (products and the scraper's run
           summary) and their log, quarantine and scrape health tables. Bronze
           stores scraped data exactly as received, all as STRING.
When     : Initial setup, after 01_create_dataset.sql. Safe to re-run.
Warning  : Changes to columns here are not applied to existing tables.
           Use ALTER TABLE ... ADD COLUMN IF NOT EXISTS instead.
===============================================================================
*/

-- -----------------------------------------------------------------------------
-- bronze_raw_products: raw scraped rows, one batch per CSV load
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS statement_competitor_data_warehouse.bronze_raw_products (
    raw_shop_name STRING,
    raw_latitude STRING,
    raw_longitude STRING,
    raw_region STRING,
    raw_city STRING,
    raw_category STRING,
    raw_title STRING,
    raw_price STRING,
    raw_description STRING,
    raw_makers STRING,
    raw_primary_material STRING,
    raw_era STRING,
    raw_condition STRING,
    raw_dimension STRING,
    raw_url STRING,
    raw_available STRING,
    scrape_timestamp STRING,
    execution_id STRING,
    batch_id STRING,
    ingested_at TIMESTAMP
);

-- -----------------------------------------------------------------------------
-- bronze_raw_error_logs: start, completion and error events of Bronze procedures
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS statement_competitor_data_warehouse.bronze_raw_error_logs (
    log_id STRING,
    execution_id STRING,
    batch_id STRING,
    error_timestamp TIMESTAMP,
    scraper_job_id STRING,
    error_type STRING,
    error_details STRING
);

-- -----------------------------------------------------------------------------
-- bronze_quarantine_bad_records: rows failing Bronze validation (as JSON)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS statement_competitor_data_warehouse.bronze_quarantine_bad_records (
    quarantine_id STRING,
    execution_id STRING,
    batch_id STRING,
    error_reason STRING,
    quarantine_timestamp TIMESTAMP,
    raw_payload STRING
);

-- -----------------------------------------------------------------------------
-- bronze_validity_log: row counts per validated batch
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS statement_competitor_data_warehouse.bronze_validity_log (
    log_id STRING,
    execution_id STRING,
    batch_id STRING,
    validation_timestamp TIMESTAMP,
    records_processed INT64,
    records_passed INT64,
    records_quarantined INT64,
    duration_seconds INT64
);

-- -----------------------------------------------------------------------------
-- bronze_scrape_health: one row per shop per batch with the scrape health result.
-- FAILED scrapes are ignored by the Gold delisting logic.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS statement_competitor_data_warehouse.bronze_scrape_health (
    health_id STRING OPTIONS(description="Unique identifier (UUID)"),
    execution_id STRING OPTIONS(description="sp_check_scrape_health run identifier"),
    batch_id STRING OPTIONS(description="Bronze batch checked"),
    shop_name STRING OPTIONS(description="Shop name as scraped (trimmed)"),
    checked_at TIMESTAMP OPTIONS(description="When the check ran"),
    status STRING OPTIONS(description="HEALTHY, WARNING or FAILED"),
    issues STRING OPTIONS(description="Failed checks, separated by '; '"),
    product_count INT64 OPTIONS(description="Distinct product URLs in this scrape (NULL if the shop is missing)"),
    baseline_count INT64 OPTIONS(description="Median product count of the shop's last 3 non-failed scrapes"),
    quarantine_rate FLOAT64 OPTIONS(description="Share of the shop's rows quarantined by Bronze validation"),
    duplicate_url_rate FLOAT64 OPTIONS(description="Share of the shop's rows with a repeated URL"),
    latest_scrape_ts TIMESTAMP OPTIONS(description="Latest scrape timestamp in this scrape"),
    details STRING OPTIONS(description="Measured values behind the result"),
    scraper_status STRING OPTIONS(description="Status reported by the scraper run summary (SUCCESS, PARTIAL, FAILED)"),
    products_found INT64 OPTIONS(description="Products the scraper reported finding"),
    products_expected INT64 OPTIONS(description="Products the shop's site reported listing")
);

-- Add the run summary columns to an existing table.
ALTER TABLE statement_competitor_data_warehouse.bronze_scrape_health
    ADD COLUMN IF NOT EXISTS scraper_status STRING OPTIONS(description="Status reported by the scraper run summary (SUCCESS, PARTIAL, FAILED)"),
    ADD COLUMN IF NOT EXISTS products_found INT64 OPTIONS(description="Products the scraper reported finding"),
    ADD COLUMN IF NOT EXISTS products_expected INT64 OPTIONS(description="Products the shop's site reported listing");

-- -----------------------------------------------------------------------------
-- bronze_scrape_run_summary: the scraper's own report, one row per shop per run,
-- loaded unchanged from <products file>_run_summary.csv
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS statement_competitor_data_warehouse.bronze_scrape_run_summary (
    raw_run_id STRING,
    raw_shop_name STRING,
    raw_started_at STRING,
    raw_finished_at STRING,
    raw_status STRING,
    raw_pages_scraped STRING,
    raw_products_found STRING,
    raw_products_expected STRING,
    raw_http_errors STRING,
    raw_retries STRING,
    raw_error_message STRING,
    raw_scraper_version STRING,
    source_uri STRING,
    execution_id STRING,
    batch_id STRING,
    ingested_at TIMESTAMP
);


/*
===============================================================================
Tests (run manually)
===============================================================================
-- All six Bronze tables exist.
SELECT table_name, creation_time
FROM `statement_competitor_data_warehouse.INFORMATION_SCHEMA.TABLES`
WHERE table_name LIKE 'bronze_%'
ORDER BY table_name;
*/
