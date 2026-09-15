/*
=============================================================================
1. CREATE DATASET (DATABASE)
=============================================================================
In BigQuery, a 'SCHEMA' or 'DATABASE' is called a Dataset.
That is why there is one 'statement_data_warehouse' dataset(schema) and prefixes
for the tables with bronze_, silver_, and gold_ to maintain the Medallion Architecture.

Purpose of the script:
    This script checks if the database exists and creates a new database if it does not exist.

Warning:
    Deleting the IF NOT EXISTS statement and running the code will result in the deletion of all the data. Proceed with
    caution.
*/

CREATE SCHEMA IF NOT EXISTS statement_data_warehouse;


/* 
=============================================================================
2. BRONZE LAYER (Landing / Raw Data)
=============================================================================
*/

CREATE OR REPLACE TABLE statement_competitor_data_warehouse.bronze_raw_products (
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
    scrape_timestamp DATETIME
);

-- This is the scraper error logging table 
CREATE OR REPLACE TABLE statement_competitor_data_warehouse.bronze_raw_error_logs (
    log_id STRING,
    error_timestamp TIMESTAMP,
    scraper_job_id STRING,
    error_type STRING, -- e.g., 'network', 'DOM_change', 'timeout'
    error_details STRING
);

/* 
=============================================================================
3. SILVER LAYER (Staging & Validation)
=============================================================================
*/

-- Quarantine table for malformed data (e.g., NULL URLs, unparseable prices)
CREATE OR REPLACE TABLE statement_competitor_data_warehouse.silver_quarantine_bad_records (
    error_reason STRING,
    quarantine_timestamp TIMESTAMP,
    raw_payload STRING -- Can store the full row as JSON or string for debugging
);

-- Validity log table 
CREATE OR REPLACE TABLE statement_competitor_data_warehouse.silver_validity_log (
    log_id STRING,
    validation_timestamp TIMESTAMP,
    records_processed INT64,
    records_passed INT64,
    records_quarantined INT64
);

-- Staging View for valid records 
-- This view acts as the clean source for the Gold MERGE operation
CREATE OR REPLACE VIEW statement_competitor_data_warehouse.silver_stg_clean_products AS
SELECT 
    *
FROM statement_competitor_data_warehouse.bronze_raw_products
WHERE raw_url IS NOT NULL 
  AND raw_price IS NOT NULL;


/* 
=============================================================================
4. GOLD LAYER (Business-Ready Star Schema)
=============================================================================
*/

-- DIMENSION: Shop (Updated IDs to INT64)
CREATE OR REPLACE TABLE statement_competitor_data_warehouse.gold_dim_shop (
    shop_id INT64 NOT NULL,
    shop_name STRING,
    region STRING,
    city STRING,
    latitude FLOAT64,
    longitude FLOAT64,
    PRIMARY KEY (shop_id) NOT ENFORCED
);

-- DIMENSION: Material
CREATE OR REPLACE TABLE statement_competitor_data_warehouse.gold_dim_material (
    material_id INT64 NOT NULL,
    material_name STRING,
    PRIMARY KEY (material_id) NOT ENFORCED
);

-- DIMENSION: Maker
CREATE OR REPLACE TABLE statement_competitor_data_warehouse.gold_dim_maker (
    maker_id INT64 NOT NULL,
    maker_name STRING,
    PRIMARY KEY (maker_id) NOT ENFORCED
);

-- DIMENSION: Category
CREATE OR REPLACE TABLE statement_competitor_data_warehouse.gold_dim_category (
    category_id INT64 NOT NULL,
    category_name STRING,
    PRIMARY KEY (category_id) NOT ENFORCED
);

-- FACT (Core Product Hub): fact_product_scrape
CREATE OR REPLACE TABLE statement_competitor_data_warehouse.gold_fact_product_scrape (
    product_id INT64 NOT NULL,
    shop_id INT64,
    category_id INT64,
    maker_id INT64,
    material_id INT64,
    url STRING NOT NULL,
    title STRING,
    description STRING,
    era STRING,
    condition STRING,
    dimension STRING,
    PRIMARY KEY (product_id) NOT ENFORCED,
    FOREIGN KEY (shop_id) REFERENCES statement_competitor_data_warehouse.gold_dim_shop(shop_id) NOT ENFORCED,
    FOREIGN KEY (category_id) REFERENCES statement_competitor_data_warehouse.gold_dim_category(category_id) NOT ENFORCED,
    FOREIGN KEY (maker_id) REFERENCES statement_competitor_data_warehouse.gold_dim_maker(maker_id) NOT ENFORCED,
    FOREIGN KEY (material_id) REFERENCES statement_competitor_data_warehouse.gold_dim_material(material_id) NOT ENFORCED
);

-- DIMENSION / FACT HISTORY: dim_price_history (Type 2 SCD / Snapshot log)
CREATE OR REPLACE TABLE statement_competitor_data_warehouse.gold_dim_price_history (
    price_log_id INT64 NOT NULL,
    product_id INT64,
    scrape_timestamp TIMESTAMP,
    price_zar NUMERIC, -- NUMERIC which is best practice for financial data
    available BOOLEAN,
    PRIMARY KEY (price_log_id) NOT ENFORCED,
    FOREIGN KEY (product_id) REFERENCES statement_competitor_data_warehouse.gold_fact_product_scrape(product_id) NOT ENFORCED
);
