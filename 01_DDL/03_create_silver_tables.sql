/*
===============================================================================
Script   : 03_create_silver_tables.sql
Layer    : Silver
Purpose  : Creates the Silver tables: cleaned products, star schema (dimensions
           and fact), price history (SCD Type 2), and their log and quarantine
           tables.
When     : Initial setup, after 02_create_bronze_tables.sql. Safe to re-run.
Warning  : Changes to columns here are not applied to existing tables.
           Use ALTER TABLE ... ADD COLUMN IF NOT EXISTS instead.
           Never DROP or CREATE OR REPLACE dim_price_history: its history
           cannot be rebuilt.
===============================================================================
*/

-- =============================================================================
-- 1. Cleaned products
-- =============================================================================

-- -----------------------------------------------------------------------------
-- silver_transformed_products: cleaned, typed products (one row per URL)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS statement_competitor_data_warehouse.silver_transformed_products (
    shop_name STRING OPTIONS(description="Name of the retail store or antique shop selling the product"),
    latitude FLOAT64 OPTIONS(description="Geographical latitude coordinate of shop location"),
    longitude FLOAT64 OPTIONS(description="Geographical longitude coordinate of shop location"),
    region STRING OPTIONS(description="Province or geographical region in South Africa"),
    city STRING OPTIONS(description="Specific city or town of the shop"),
    category STRING OPTIONS(description="Overarching furniture or decor category"),
    title STRING OPTIONS(description="Primary listing title or name of product"),
    price NUMERIC OPTIONS(description="Listed selling price in ZAR (DECIMAL 10,2)"),
    description STRING OPTIONS(description="Full body text detailing product features and history"),
    makers STRING OPTIONS(description="Manufacturer, designer, or brand of item"),
    primary_material STRING OPTIONS(description="Dominant material used in construction"),
    era STRING OPTIONS(description="Historical time period or design movement"),
    condition STRING OPTIONS(description="Physical state/condition of the item"),
    dimensions STRING OPTIONS(description="Physical measurements (Length, Width, Height)"),
    url STRING OPTIONS(description="Direct web link to listing (Natural Key)"),
    available BOOL OPTIONS(description="Availability flag (TRUE = Available, FALSE = Sold/Unavailable)"),
    timestamp_date DATE OPTIONS(description="Calendar date when the product listing was captured"),
    timestamp_time TIME OPTIONS(description="Time of day when the product listing was captured"),
    execution_id STRING OPTIONS(description="UUID generated during stored procedure execution"),
    batch_id STRING OPTIONS(description="Batch identifier for pipeline execution tracking"),
    ingested_at DATETIME OPTIONS(description="Timestamp when record was transformed and loaded into Silver")
)
PARTITION BY DATE(ingested_at)
CLUSTER BY shop_name, category, makers, primary_material;

-- -----------------------------------------------------------------------------
-- silver_transformed_error_logs: events of the Silver load and validation
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS statement_competitor_data_warehouse.silver_transformed_error_logs (
    log_id STRING OPTIONS(description="Unique log identifier (UUID)"),
    execution_id STRING OPTIONS(description="Pipeline execution identifier"),
    batch_id STRING OPTIONS(description="Batch load identifier"),
    error_timestamp TIMESTAMP OPTIONS(description="Timestamp when error was encountered"),
    scraper_job_id STRING OPTIONS(description="Procedure or job component name triggering the error"),
    error_type STRING OPTIONS(description="Category of error (e.g., CAST_ERROR, SCHEMA_MISMATCH)"),
    error_details STRING OPTIONS(description="Detailed error message and stack trace context")
)
PARTITION BY DATE(error_timestamp);

-- -----------------------------------------------------------------------------
-- silver_quarantine_bad_records: rows failing Silver validation (as JSON)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS statement_competitor_data_warehouse.silver_quarantine_bad_records (
    quarantine_id STRING OPTIONS(description="Unique quarantine record identifier (UUID)"),
    execution_id STRING OPTIONS(description="Pipeline execution identifier"),
    batch_id STRING OPTIONS(description="Batch load identifier"),
    error_reason STRING OPTIONS(description="Validation rule failure description"),
    quarantine_timestamp TIMESTAMP OPTIONS(description="Timestamp when record was quarantined"),
    raw_payload STRING OPTIONS(description="Raw record string/JSON payload for debugging")
)
PARTITION BY DATE(quarantine_timestamp);

-- -----------------------------------------------------------------------------
-- silver_validity_log: row counts per Silver validation run
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS statement_competitor_data_warehouse.silver_validity_log (
    log_id STRING OPTIONS(description="Unique validation log identifier (UUID)"),
    execution_id STRING OPTIONS(description="Pipeline execution identifier"),
    batch_id STRING OPTIONS(description="Batch load identifier"),
    validation_timestamp TIMESTAMP OPTIONS(description="Timestamp when batch validation completed"),
    records_processed INT64 OPTIONS(description="Total records processed during Silver run"),
    records_passed INT64 OPTIONS(description="Count of records successfully written to Silver table"),
    records_quarantined INT64 OPTIONS(description="Count of records routed to quarantine table"),
    duration_seconds INT64 OPTIONS(description="Total run duration in seconds")
)
PARTITION BY DATE(validation_timestamp);


-- =============================================================================
-- 2. Star schema
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Dimensions: keys are FARM_FINGERPRINT hashes of the normalised value
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS statement_competitor_data_warehouse.dim_shop (
    shop_id INT64 OPTIONS(description="Surrogate Primary Key (Hash)"),
    shop_name STRING,
    region STRING,
    city STRING,
    latitude FLOAT64,
    longitude FLOAT64
);

CREATE TABLE IF NOT EXISTS statement_competitor_data_warehouse.dim_category (
    category_id INT64 OPTIONS(description="Surrogate Primary Key (Hash)"),
    category STRING
);

CREATE TABLE IF NOT EXISTS statement_competitor_data_warehouse.dim_maker (
    maker_id INT64 OPTIONS(description="Surrogate Primary Key (Hash)"),
    makers STRING
);

CREATE TABLE IF NOT EXISTS statement_competitor_data_warehouse.dim_material (
    material_id INT64 OPTIONS(description="Surrogate Primary Key (Hash)"),
    primary_material STRING
);

-- -----------------------------------------------------------------------------
-- fact_product_scrape: one row per product with dimension keys
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS statement_competitor_data_warehouse.fact_product_scrape (
    product_id INT64 OPTIONS(description="Primary Key (Hash of URL)"),
    shop_id INT64 OPTIONS(description="Foreign Key to dim_shop"),
    category_id INT64 OPTIONS(description="Foreign Key to dim_category"),
    maker_id INT64 OPTIONS(description="Foreign Key to dim_maker"),
    material_id INT64 OPTIONS(description="Foreign Key to dim_material"),
    url STRING OPTIONS(description="Unique Key / Natural Key"),
    title STRING,
    description STRING,
    era STRING,
    condition STRING,
    dimensions STRING,
    execution_id STRING OPTIONS(description="Pipeline execution tracking ID"),
    batch_id STRING OPTIONS(description="Batch load tracking ID"),
    ingested_at DATETIME OPTIONS(description="Timestamp of normalisation")
)
CLUSTER BY shop_id, category_id, maker_id;

-- -----------------------------------------------------------------------------
-- silver_normalised_error_logs: events of the normalisation procedure
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS statement_competitor_data_warehouse.silver_normalised_error_logs (
    log_id STRING OPTIONS(description="Unique log UUID"),
    execution_id STRING,
    batch_id STRING,
    error_timestamp TIMESTAMP,
    procedure_name STRING,
    error_type STRING,
    error_details STRING
)
PARTITION BY DATE(error_timestamp);


-- =============================================================================
-- 3. Price history (SCD Type 2)
-- =============================================================================

-- -----------------------------------------------------------------------------
-- dim_price_history: one row per price/availability version per product.
-- Accumulates over time and cannot be rebuilt from Silver.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS statement_competitor_data_warehouse.dim_price_history (
    -- Keys
    price_log_id INT64 OPTIONS(description="Primary Key (PK): Unique surrogate identifier for this specific price history log entry"),
    product_id INT64 OPTIONS(description="Foreign Key (FK): References the unique product_id in the fact_product_scrape table"),

    -- Tracked attributes
    scrape_timestamp TIMESTAMP OPTIONS(description="The original timestamp when this exact price and availability was scraped from the source"),
    price NUMERIC OPTIONS(description="The listed selling price of the item in ZAR at the time of the scrape (Mapped to DECIMAL)"),
    available BOOL OPTIONS(description="Boolean value checking if the item is still available (TRUE) or sold (FALSE)"),

    -- SCD Type 2 columns
    valid_from TIMESTAMP OPTIONS(description="SCD Type 2: The exact timestamp from which this price/availability record became active"),
    valid_to TIMESTAMP OPTIONS(description="SCD Type 2: The timestamp when this record was superseded by a new change (9999-12-31 23:59:59 UTC if currently active)"),
    is_current BOOL OPTIONS(description="SCD Type 2: Boolean flag indicating if this is the most recent, active price record for the product"),

    -- Audit columns
    execution_id STRING OPTIONS(description="Generated UUID appended when the stored procedure runs for traceability"),
    batch_id STRING OPTIONS(description="Generated batch identifier for pipeline execution tracking"),
    ingested_at DATETIME OPTIONS(description="Timestamp indicating when this specific row was inserted into dim_price_history")
)
PARTITION BY DATE(valid_from)
CLUSTER BY product_id, is_current;

-- -----------------------------------------------------------------------------
-- silver_historisation_error_logs: events of the price history procedure
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS statement_competitor_data_warehouse.silver_historisation_error_logs (
    log_id STRING OPTIONS(description="Unique identifier for the error log entry (UUID)"),
    execution_id STRING OPTIONS(description="Pipeline execution identifier linking to the stored procedure run"),
    batch_id STRING OPTIONS(description="Batch load identifier for grouping errors by execution run"),
    error_timestamp TIMESTAMP OPTIONS(description="The exact timestamp when the error or informational event occurred"),
    procedure_name STRING OPTIONS(description="The name of the stored procedure or component that triggered the log"),
    error_type STRING OPTIONS(description="Categorical type of error (e.g., SCD2_INSERT_FAILURE, UPDATE_FAILURE, INFO)"),
    error_details STRING OPTIONS(description="Detailed error message, stack trace, or informational payload context")
)
PARTITION BY DATE(error_timestamp);


-- =============================================================================
-- 4. Reconciliation
-- =============================================================================

-- -----------------------------------------------------------------------------
-- silver_reconciled_error_logs: failed checks and events of the reconciliation
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS statement_competitor_data_warehouse.silver_reconciled_error_logs (
    log_id STRING OPTIONS(description="Unique identifier for the log entry (UUID)"),
    execution_id STRING OPTIONS(description="Pipeline execution identifier linking to the stored procedure run"),
    batch_id STRING OPTIONS(description="Batch load identifier for grouping errors by execution run"),
    error_timestamp TIMESTAMP OPTIONS(description="Timestamp when the reconciliation error was flagged"),
    procedure_name STRING OPTIONS(description="Name of the procedure logging the error"),
    error_type STRING OPTIONS(description="Categorical type of error (e.g., DATA_DROP_MISMATCH, ORPHAN_KEY, FINANCIAL_ANOMALY)"),
    error_details STRING OPTIONS(description="Detailed error message and context metrics")
)
PARTITION BY DATE(error_timestamp);


/*
===============================================================================
Tests (run manually)
===============================================================================
-- All Silver tables exist (expect 13 rows).
SELECT table_name, creation_time
FROM `statement_competitor_data_warehouse.INFORMATION_SCHEMA.TABLES`
WHERE table_type = 'BASE TABLE'
  AND REGEXP_CONTAINS(table_name, r'^(silver_|dim_|fact_)')
ORDER BY table_name;

-- Partitioning and clustering are set on the main tables.
SELECT table_name, column_name, is_partitioning_column, clustering_ordinal_position
FROM `statement_competitor_data_warehouse.INFORMATION_SCHEMA.COLUMNS`
WHERE table_name IN ('silver_transformed_products', 'fact_product_scrape', 'dim_price_history')
  AND (is_partitioning_column = 'YES' OR clustering_ordinal_position IS NOT NULL)
ORDER BY table_name, clustering_ordinal_position;
*/
