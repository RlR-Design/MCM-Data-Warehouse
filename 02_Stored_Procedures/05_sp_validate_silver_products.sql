/*
===============================================================================
Script   : 05_sp_validate_silver_products.sql
Layer    : Silver
Purpose  : Validates only the rows written by one Silver load. Rows that fail
           are copied to silver_quarantine_bad_records and deleted from Silver:
             1. URL, shop name and price present
             2. Price between 0 and 999,999.99
When     : Deploy at setup and after any change. Runs as step 5 of sp_run_pipeline.
Requires : 04_sp_load_silver_products has completed.
Params   : p_silver_batch_id  Silver load batch (BATCH_SLV_...). NULL = latest completed load.
Logs     : silver_transformed_error_logs, silver_validity_log
Warning  : A product whose latest scrape fails validation is removed from Silver,
           even if an earlier version was valid. Safe to re-run for a batch.
===============================================================================
*/

CREATE OR REPLACE PROCEDURE statement_competitor_data_warehouse.sp_validate_silver_products(IN p_silver_batch_id STRING)
OPTIONS(strict_mode=false)
BEGIN
    DECLARE start_time TIMESTAMP;
    DECLARE v_execution_id STRING;
    DECLARE v_batch_id STRING;
    DECLARE v_silver_batch_id STRING;
    DECLARE v_error_msg STRING;
    DECLARE v_error_type STRING;
    DECLARE v_records_processed INT64 DEFAULT 0;
    DECLARE v_records_passed INT64 DEFAULT 0;
    DECLARE v_records_quarantined INT64 DEFAULT 0;

    -- Initialise run identifiers.
    SET start_time = CURRENT_TIMESTAMP();
    SET v_execution_id = GENERATE_UUID();
    SET v_batch_id = FORMAT_TIMESTAMP('BATCH_VAL_%Y%m%d_%H%M%S', start_time);

    -- Resolve the Silver load batch: the parameter, else the latest completed load.
    SET v_silver_batch_id = COALESCE(
        p_silver_batch_id,
        (
            SELECT batch_id
            FROM statement_competitor_data_warehouse.silver_transformed_error_logs
            WHERE scraper_job_id = 'sp_load_silver_products'
              AND error_type = 'INFO_STAGE_COMPLETE'
            ORDER BY error_timestamp DESC
            LIMIT 1
        )
    );

    -- Log start.
    INSERT INTO statement_competitor_data_warehouse.silver_transformed_error_logs (
        log_id, execution_id, batch_id, error_timestamp, scraper_job_id, error_type, error_details
    )
    VALUES (
        GENERATE_UUID(), v_execution_id, v_batch_id, CURRENT_TIMESTAMP(),
        'sp_validate_silver_products', 'INFO_STAGE_START',
        FORMAT('Starting Silver Layer Validation for Silver load batch %s.', COALESCE(v_silver_batch_id, 'NONE'))
    );

    BEGIN
        -- Stop if there is no Silver load to validate.
        IF v_silver_batch_id IS NULL THEN
            RAISE USING MESSAGE = 'No completed sp_load_silver_products run found to validate.';
        END IF;

        -- Evaluate the rules for every row written by the Silver load.
        CREATE OR REPLACE TEMP TABLE stg_validated_unprocessed AS
        WITH evaluated_source AS (
            SELECT
                b.url,

                -- Mandatory fields.
                (b.url IS NULL OR TRIM(b.url) = '' OR UPPER(TRIM(b.url)) IN ('NULL', 'NONE', 'NAN', 'N/A', '-')) AS is_missing_url,
                (b.shop_name IS NULL OR TRIM(b.shop_name) = '' OR UPPER(TRIM(b.shop_name)) IN ('NULL', 'NONE', 'NAN', 'N/A', '-', 'UNKNOWN')) AS is_missing_shop,
                (b.price IS NULL) AS is_missing_price,

                -- Price range.
                (b.price < 0 OR b.price >= 1000000) AS is_price_out_of_range,

                -- Row snapshot for the quarantine table.
                TO_JSON_STRING(STRUCT(
                    b.shop_name, b.latitude, b.longitude, b.region, b.city,
                    b.category, b.title, b.price, b.description, b.makers,
                    b.primary_material, b.era, b.condition, b.dimensions,
                    b.url, b.available, b.timestamp_date, b.timestamp_time, b.batch_id
                )) AS raw_payload
            FROM statement_competitor_data_warehouse.silver_transformed_products b
            WHERE b.batch_id = v_silver_batch_id
        )
        SELECT
            *,
            NOT (is_missing_url OR is_missing_shop OR is_missing_price OR COALESCE(is_price_out_of_range, FALSE)) AS is_valid
        FROM evaluated_source;

        SET v_records_processed = (SELECT COUNT(*) FROM stg_validated_unprocessed);

        -- Copy failing rows to quarantine with their reasons.
        INSERT INTO statement_competitor_data_warehouse.silver_quarantine_bad_records (
            quarantine_id, execution_id, batch_id, error_reason, quarantine_timestamp, raw_payload
        )
        SELECT
            GENERATE_UUID(),
            v_execution_id,
            v_batch_id,
            CONCAT(
                CASE WHEN is_missing_url THEN 'MISSING_URL; ' ELSE '' END,
                CASE WHEN is_missing_shop THEN 'MISSING_SHOP_NAME; ' ELSE '' END,
                CASE WHEN is_missing_price THEN 'MISSING_PRICE; ' ELSE '' END,
                CASE WHEN COALESCE(is_price_out_of_range, FALSE) THEN 'PRICE_OUT_OF_RANGE; ' ELSE '' END
            ),
            CURRENT_TIMESTAMP(),
            raw_payload
        FROM stg_validated_unprocessed
        WHERE NOT is_valid;

        SET v_records_quarantined = (SELECT COUNT(*) FROM stg_validated_unprocessed WHERE NOT is_valid);
        SET v_records_passed = v_records_processed - v_records_quarantined;

        -- Delete failing rows from Silver so they exist only in quarantine.
        DELETE FROM statement_competitor_data_warehouse.silver_transformed_products
        WHERE batch_id = v_silver_batch_id
          AND (
              url IS NULL
              OR url IN (SELECT url FROM stg_validated_unprocessed WHERE NOT is_valid AND url IS NOT NULL)
          );

        -- Record the validation result.
        INSERT INTO statement_competitor_data_warehouse.silver_validity_log (
            log_id, execution_id, batch_id, validation_timestamp,
            records_processed, records_passed, records_quarantined, duration_seconds
        )
        VALUES (
            GENERATE_UUID(), v_execution_id, v_batch_id, CURRENT_TIMESTAMP(),
            v_records_processed, v_records_passed, v_records_quarantined,
            TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), start_time, SECOND)
        );

    EXCEPTION WHEN ERROR THEN
        -- Capture the error.
        SET v_error_msg = FORMAT('Message: %s | Statement: %s', COALESCE(@@error.message, 'Unknown error'), COALESCE(@@error.statement_text, 'Unknown statement'));
        SET v_error_type = 'VALIDATION_PROCEDURE_FAILURE';

        -- Log the error.
        INSERT INTO statement_competitor_data_warehouse.silver_transformed_error_logs (
            log_id, execution_id, batch_id, error_timestamp, scraper_job_id, error_type, error_details
        )
        VALUES (
            GENERATE_UUID(), v_execution_id, v_batch_id, CURRENT_TIMESTAMP(),
            'sp_validate_silver_products', v_error_type, v_error_msg
        );

        -- Re-raise so the caller sees the failure.
        RAISE USING MESSAGE = FORMAT('sp_validate_silver_products failed [Execution ID: %s]: %s', v_execution_id, v_error_msg);
    END;

    -- Log completion.
    INSERT INTO statement_competitor_data_warehouse.silver_transformed_error_logs (
        log_id, execution_id, batch_id, error_timestamp, scraper_job_id, error_type, error_details
    )
    VALUES (
        GENERATE_UUID(), v_execution_id, v_batch_id, CURRENT_TIMESTAMP(),
        'sp_validate_silver_products', 'INFO_STAGE_COMPLETE',
        FORMAT('Silver Validation COMPLETED for Silver load batch %s. Processed: %d, Passed: %d, Quarantined: %d in %d seconds',
               v_silver_batch_id, v_records_processed, v_records_passed, v_records_quarantined, TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), start_time, SECOND))
    );
END;


/*
===============================================================================
Tests (run manually)
===============================================================================
-- Validate the latest Silver load.
CALL statement_competitor_data_warehouse.sp_validate_silver_products(NULL);

-- Validate a specific Silver load.
CALL statement_competitor_data_warehouse.sp_validate_silver_products('BATCH_SLV_YYYYMMDD_HHMMSS');

-- Validation result per run (processed = passed + quarantined).
SELECT validation_timestamp, records_processed, records_passed, records_quarantined
FROM statement_competitor_data_warehouse.silver_validity_log
ORDER BY validation_timestamp DESC
LIMIT 10;

-- Quarantine reasons.
SELECT error_reason, COUNT(*) AS n, ANY_VALUE(raw_payload) AS example
FROM statement_competitor_data_warehouse.silver_quarantine_bad_records
GROUP BY error_reason
ORDER BY n DESC;

-- No Silver rows break the mandatory rules (expect 0).
SELECT COUNT(*) AS invalid_rows
FROM statement_competitor_data_warehouse.silver_transformed_products
WHERE shop_name IS NULL
   OR UPPER(TRIM(shop_name)) IN ('', 'NULL', 'NONE', 'NAN', 'N/A', '-', 'UNKNOWN')
   OR price IS NULL
   OR price < 0
   OR price >= 1000000;
*/
