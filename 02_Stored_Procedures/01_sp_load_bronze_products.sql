/*
===============================================================================
Script   : 01_sp_load_bronze_products.sql
Layer    : Bronze
Purpose  : Loads a scraped CSV from Cloud Storage and appends it unchanged to
           bronze_raw_products as a new batch.
When     : Deploy at setup and after any change. Runs as step 1 of sp_run_pipeline.
Requires : 02_create_bronze_tables.sql
Params   : p_gcs_uri  CSV location. NULL = gs://statement_competitor_data_warehouse/MCM_All_products.csv
Logs     : bronze_raw_error_logs
Warning  : Every CALL appends the whole file. Calling twice with the same file
           loads it twice.
===============================================================================
*/

CREATE OR REPLACE PROCEDURE statement_competitor_data_warehouse.sp_load_bronze_products (IN p_gcs_uri STRING)
BEGIN
    DECLARE evaluated_uri STRING;
    DECLARE start_time TIMESTAMP;
    DECLARE v_execution_id STRING;
    DECLARE v_batch_id STRING;
    DECLARE v_error_msg STRING;
    DECLARE v_error_type STRING;

    -- Initialise run identifiers and resolve the file location.
    SET start_time = CURRENT_TIMESTAMP();
    SET v_execution_id = GENERATE_UUID();
    SET v_batch_id = FORMAT_TIMESTAMP('BATCH_%Y%m%d_%H%M%S', start_time);
    SET evaluated_uri = COALESCE(p_gcs_uri, 'gs://statement_competitor_data_warehouse/MCM_All_products.csv');

    -- Log start.
    INSERT INTO statement_competitor_data_warehouse.bronze_raw_error_logs (
        log_id, execution_id, batch_id, error_timestamp, scraper_job_id, error_type, error_details
    )
    VALUES (
        GENERATE_UUID(), v_execution_id, v_batch_id, CURRENT_TIMESTAMP(),
        'sp_load_bronze_products', 'INFO_STAGE_START', FORMAT('[1/3 & 2/3] INITIALISING & LOADING from %s', evaluated_uri)
    );

    BEGIN
        -- Create a session staging table matching the CSV columns.
        CREATE OR REPLACE TEMP TABLE stg_bronze_raw_products (
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
            scrape_timestamp STRING
        );

        -- Load the CSV into the staging table (dynamic SQL: the URI is a parameter).
        EXECUTE IMMEDIATE FORMAT("""
            LOAD DATA OVERWRITE TEMP TABLE stg_bronze_raw_products (
                raw_shop_name STRING, raw_latitude STRING, raw_longitude STRING, raw_region STRING,
                raw_city STRING, raw_category STRING, raw_title STRING, raw_price STRING,
                raw_description STRING, raw_makers STRING, raw_primary_material STRING,
                raw_era STRING, raw_condition STRING, raw_dimension STRING, raw_url STRING,
                raw_available STRING, scrape_timestamp STRING
            )
            FROM FILES (
                format = 'CSV',
                uris = [%T],
                skip_leading_rows = 1,
                field_delimiter = ',',
                quote = '"',
                allow_quoted_newlines = true
            )
        """, evaluated_uri);

        -- Append the staged rows to Bronze with audit columns.
        INSERT INTO statement_competitor_data_warehouse.bronze_raw_products (
            raw_shop_name,
            raw_latitude,
            raw_longitude,
            raw_region,
            raw_city,
            raw_category,
            raw_title,
            raw_price,
            raw_description,
            raw_makers,
            raw_primary_material,
            raw_era,
            raw_condition,
            raw_dimension,
            raw_url,
            raw_available,
            scrape_timestamp,
            execution_id,
            batch_id,
            ingested_at
        )
        SELECT
            raw_shop_name,
            raw_latitude,
            raw_longitude,
            raw_region,
            raw_city,
            raw_category,
            raw_title,
            raw_price,
            raw_description,
            raw_makers,
            raw_primary_material,
            raw_era,
            raw_condition,
            raw_dimension,
            raw_url,
            raw_available,
            scrape_timestamp,
            v_execution_id AS execution_id,
            v_batch_id AS batch_id,
            CURRENT_TIMESTAMP() AS ingested_at
        FROM stg_bronze_raw_products;

    EXCEPTION WHEN ERROR THEN
        -- Capture and classify the error.
        SET v_error_msg = FORMAT(
            'Message: %s | Statement: %s',
            COALESCE(@@error.message, 'Unknown error occurred'),
            COALESCE(@@error.statement_text, 'Unknown statement')
        );

        SET v_error_type = CASE
            WHEN LOWER(v_error_msg) LIKE '%schema%' OR LOWER(v_error_msg) LIKE '%type%' OR LOWER(v_error_msg) LIKE '%column%' OR LOWER(v_error_msg) LIKE '%mismatch%' THEN 'SCHEMA_MISMATCH'
            WHEN LOWER(v_error_msg) LIKE '%parse%' OR LOWER(v_error_msg) LIKE '%delimiter%' OR LOWER(v_error_msg) LIKE '%corrupt%' OR LOWER(v_error_msg) LIKE '%format%' THEN 'PARSING_FAILURE'
            WHEN LOWER(v_error_msg) LIKE '%access%' OR LOWER(v_error_msg) LIKE '%permission%' OR LOWER(v_error_msg) LIKE '%denied%' OR LOWER(v_error_msg) LIKE '%not found%' OR LOWER(v_error_msg) LIKE '%forbidden%' THEN 'ACCESS_DENIED'
            ELSE 'BULK_LOAD_FAILURE'
        END;

        -- Log the error.
        INSERT INTO statement_competitor_data_warehouse.bronze_raw_error_logs (
            log_id, execution_id, batch_id, error_timestamp, scraper_job_id, error_type, error_details
        )
        VALUES (
            GENERATE_UUID(), v_execution_id, v_batch_id, CURRENT_TIMESTAMP(),
            'sp_load_bronze_products', v_error_type, v_error_msg
        );

        -- Re-raise so the caller sees the failure.
        RAISE USING MESSAGE = FORMAT('sp_load_bronze_products failed [Execution ID: %s]: %s', v_execution_id, v_error_msg);
    END;

    -- Log completion.
    INSERT INTO statement_competitor_data_warehouse.bronze_raw_error_logs (
        log_id, execution_id, batch_id, error_timestamp, scraper_job_id, error_type, error_details
    )
    VALUES (
        GENERATE_UUID(), v_execution_id, v_batch_id, CURRENT_TIMESTAMP(),
        'sp_load_bronze_products', 'INFO_STAGE_COMPLETE', FORMAT('[3/3] COMPLETED in %d seconds', TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), start_time, SECOND))
    );
END;


/*
===============================================================================
Tests (run manually)
===============================================================================
-- Load the default CSV. Appends the whole file as a new batch.
CALL statement_competitor_data_warehouse.sp_load_bronze_products(NULL);

-- Load a specific CSV.
CALL statement_competitor_data_warehouse.sp_load_bronze_products('gs://statement_competitor_data_warehouse/<file>.csv');

-- Rows per batch. Batches with equal counts loaded close together may be duplicate loads.
SELECT batch_id, MIN(ingested_at) AS loaded_at, COUNT(*) AS row_count, COUNT(DISTINCT raw_url) AS unique_urls
FROM statement_competitor_data_warehouse.bronze_raw_products
GROUP BY batch_id
ORDER BY loaded_at DESC;

-- Latest run logged INFO_STAGE_START and INFO_STAGE_COMPLETE (no error rows).
SELECT error_timestamp, error_type, error_details
FROM statement_competitor_data_warehouse.bronze_raw_error_logs
WHERE scraper_job_id = 'sp_load_bronze_products'
ORDER BY error_timestamp DESC
LIMIT 10;

-- Price formats in Bronze (digits shown as 9).
SELECT REGEXP_REPLACE(raw_price, r'[0-9]', '9') AS price_pattern, COUNT(*) AS n, ANY_VALUE(raw_price) AS example
FROM statement_competitor_data_warehouse.bronze_raw_products
GROUP BY price_pattern
ORDER BY n DESC
LIMIT 30;

-- Availability values in Bronze.
SELECT raw_available, COUNT(*) AS n
FROM statement_competitor_data_warehouse.bronze_raw_products
GROUP BY raw_available
ORDER BY n DESC;

-- Scrape timestamp formats in Bronze (digits shown as 9).
SELECT REGEXP_REPLACE(scrape_timestamp, r'[0-9]', '9') AS ts_pattern, COUNT(*) AS n, ANY_VALUE(scrape_timestamp) AS example
FROM statement_competitor_data_warehouse.bronze_raw_products
GROUP BY ts_pattern
ORDER BY n DESC;
*/
