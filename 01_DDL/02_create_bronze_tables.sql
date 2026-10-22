/*
===============================================================================
Script   : 02_create_bronze_tables.sql
Layer    : Bronze
Purpose  : Creates the Bronze landing table and its log and quarantine tables.
           Bronze stores scraped data exactly as received, all as STRING.
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


/*
===============================================================================
Tests (run manually)
===============================================================================
-- All four Bronze tables exist.
SELECT table_name, creation_time
FROM `statement_competitor_data_warehouse.INFORMATION_SCHEMA.TABLES`
WHERE table_name LIKE 'bronze_%'
ORDER BY table_name;
*/
