/*
=============================================================================
SILVER LAYER DDL SCRIPT (BigQuery)
Dataset: statement_competitor_data_warehouse

Description:
  This script creates the physical table structures for the Silver layer.
  1. silver_transformed_products: Main cleansed, standardised, and typed dataset.
  2. silver_error_logs: Traps procedural & runtime errors during Silver ETL.
  3. silver_quarantine_bad_records: Captures malformed or invalid records.
  4. silver_validity_log: Records audit & execution summary metrics.
=============================================================================

    WARNING:
    Running this script will delete all the historical data that exists in the current table. This script should only be
    used in the event or a system failure or in the event that the system needs to be refreshed.
=============================================================================

 */

-- =============================================================================
-- 1. MAIN SILVER TRANSFORMED TABLE
-- Standardized data types applied (STRING, FLOAT64, NUMERIC, BOOL, DATETIME)
-- =============================================================================
DROP TABLE IF EXISTS statement_competitor_data_warehouse.silver_transformed_products;

CREATE TABLE statement_competitor_data_warehouse.silver_transformed_products (
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

-- =============================================================================
-- 2. SILVER ERROR LOGS TABLE
-- Matches Bronze error log structure for process failure auditing
-- =============================================================================
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

-- =============================================================================
-- 3. SILVER QUARANTINE TABLE
-- Captures rows failing validation or type casting rules during Silver ETL
-- =============================================================================
CREATE TABLE IF NOT EXISTS statement_competitor_data_warehouse.silver_quarantine_bad_records (
    quarantine_id STRING OPTIONS(description="Unique quarantine record identifier (UUID)"),
    execution_id STRING OPTIONS(description="Pipeline execution identifier"),
    batch_id STRING OPTIONS(description="Batch load identifier"),
    error_reason STRING OPTIONS(description="Validation rule failure description"),
    quarantine_timestamp TIMESTAMP OPTIONS(description="Timestamp when record was quarantined"),
    raw_payload STRING OPTIONS(description="Raw record string/JSON payload for debugging")
)
PARTITION BY DATE(quarantine_timestamp);

-- =============================================================================
-- 4. SILVER VALIDITY LOG TABLE
-- Logs row counts, execution duration, and validation health metrics
-- =============================================================================
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
-- VERIFICATION QUERY
-- Query BigQuery INFORMATION_SCHEMA to confirm all Silver tables are ready
-- =============================================================================
SELECT
    table_name,
    table_type,
    creation_time
FROM `statement_competitor_data_warehouse.INFORMATION_SCHEMA.TABLES`
WHERE table_name LIKE 'silver_%'
ORDER BY table_name;


-- STREAMING_CHUNK: Setting up DDL for Star Schema dimension tables...
/*
=============================================================================
SILVER LAYER: NORMALISATION & STAR SCHEMA DDL
Dataset: statement_competitor_data_warehouse

Description:
This script builds the dimension tables and the central normalised fact table
(`fact_product_scrape`) according to the Star Schema ER Diagram.
It also sets up the required error logging table for the normalisation process.
=============================================================================
*/

-- 1. Dimension Tables
CREATE OR REPLACE TABLE statement_competitor_data_warehouse.dim_shop (
    shop_id INT64 OPTIONS(description="Surrogate Primary Key (Hash)"),
    shop_name STRING,
    region STRING,
    city STRING,
    latitude FLOAT64,
    longitude FLOAT64
);

CREATE OR REPLACE TABLE statement_competitor_data_warehouse.dim_category (
    category_id INT64 OPTIONS(description="Surrogate Primary Key (Hash)"),
    category STRING
);

CREATE OR REPLACE TABLE statement_competitor_data_warehouse.dim_maker (
    maker_id INT64 OPTIONS(description="Surrogate Primary Key (Hash)"),
    makers STRING
);

CREATE OR REPLACE TABLE statement_competitor_data_warehouse.dim_material (
    material_id INT64 OPTIONS(description="Surrogate Primary Key (Hash)"),
    primary_material STRING
);

-- STREAMING_CHUNK: Setting up DDL for the normalised target fact table and error logs...
-- 2. Normalised Target Table (Fact Table: fact_product_scrape representation)
CREATE OR REPLACE TABLE statement_competitor_data_warehouse.fact_product_scrape (
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

-- 3. Error Logging Table
CREATE OR REPLACE TABLE statement_competitor_data_warehouse.silver_normalised_error_logs (
    log_id STRING OPTIONS(description="Unique log UUID"),
    execution_id STRING,
    batch_id STRING,
    error_timestamp TIMESTAMP,
    procedure_name STRING,
    error_type STRING,
    error_details STRING
)
PARTITION BY DATE(error_timestamp);

/*
=============================================================================
HISTORISATION TABLE DDL SCRIPT
Dataset: statement_competitor_data_warehouse
Table: dim_price_history
=============================================================================

DESCRIPTION:
This DDL script creates the `dim_price_history` table and the `silver_historisation_error_logs` table within the statement_competitor_data_warehouse. This table is
designed to track the historical state of a product's price and availability.

To capture this history effectively, it utilises a Type 2 Slowly Changing
Dimension (SCD Type 2) format. Unlike a standard table that overwrites old
data when an update occurs, an SCD Type 2 table inserts a brand new row for
every change.

=============================================================================
WARNING - DATA LOSS RISK:

Do not run this script in a production environment unless you are actively
rebuilding the warehouse from scratch, recovering from a total system failure,
or intentionally resetting the historical data structures.
=============================================================================
*/

-- STREAMING_CHUNK: Creating the primary dim_price_history table for SCD Type 2 tracking...
CREATE OR REPLACE TABLE statement_competitor_data_warehouse.dim_price_history (

    -- ==========================================
    -- Primary and Foreign Keys (From ERD)
    -- ==========================================
    price_log_id INT64 OPTIONS(description="Primary Key (PK): Unique surrogate identifier for this specific price history log entry"),
    product_id INT64 OPTIONS(description="Foreign Key (FK): References the unique product_id in the fact_product_scrape table"),

    -- ==========================================
    -- Core Tracked Attributes (From ERD)
    -- ==========================================
    scrape_timestamp TIMESTAMP OPTIONS(description="The original timestamp when this exact price and availability was scraped from the source"),
    price NUMERIC OPTIONS(description="The listed selling price of the item in ZAR at the time of the scrape (Mapped to DECIMAL)"),
    available BOOL OPTIONS(description="Boolean value checking if the item is still available (TRUE) or sold (FALSE)"),

    -- ==========================================
    -- SCD Type 2 Tracking Columns
    -- ==========================================
    valid_from TIMESTAMP OPTIONS(description="SCD Type 2: The exact timestamp from which this price/availability record became active"),
    valid_to TIMESTAMP OPTIONS(description="SCD Type 2: The timestamp when this record was superseded by a new change (NULL if currently active)"),
    is_current BOOL OPTIONS(description="SCD Type 2: Boolean flag indicating if this is the most recent, active price record for the product"),

    -- ==========================================
    -- Standard Data Warehouse Audit Columns
    -- ==========================================
    execution_id STRING OPTIONS(description="Generated UUID appended when the stored procedure runs for traceability"),
    batch_id STRING OPTIONS(description="Generated batch identifier for pipeline execution tracking"),
    ingested_at DATETIME OPTIONS(description="Timestamp indicating when this specific row was inserted into the gold layer table")

)
PARTITION BY DATE(valid_from)
CLUSTER BY product_id, is_current;

-- STREAMING_CHUNK: Creating the silver_historisation_error_logs table for pipeline auditing...
-- =============================================================================
-- ERROR LOGGING TABLE (Silver Layer Pipeline Trapping)
-- =============================================================================
CREATE OR REPLACE TABLE statement_competitor_data_warehouse.silver_historisation_error_logs (
    log_id STRING OPTIONS(description="Unique identifier for the error log entry (UUID)"),
    execution_id STRING OPTIONS(description="Pipeline execution identifier linking to the stored procedure run"),
    batch_id STRING OPTIONS(description="Batch load identifier for grouping errors by execution run"),
    error_timestamp TIMESTAMP OPTIONS(description="The exact timestamp when the error or informational event occurred"),
    procedure_name STRING OPTIONS(description="The name of the stored procedure or component that triggered the log"),
    error_type STRING OPTIONS(description="Categorical type of error (e.g., SCD2_INSERT_FAILURE, UPDATE_FAILURE, INFO)"),
    error_details STRING OPTIONS(description="Detailed error message, stack trace, or informational payload context")
)
PARTITION BY DATE(error_timestamp);


/*
=============================================================================
SILVER LAYER: RECONCILIATION & VALIDATION PROCEDURE

If anomalies are detected, they are logged into `silver_reconciled_error_logs`.
=============================================================================
 */

 -- STREAMING_CHUNK: Creating the reconciliation error logs table for audit tracking...
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


