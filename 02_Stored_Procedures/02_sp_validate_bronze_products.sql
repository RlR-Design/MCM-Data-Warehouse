/*
===============================================================================
Script   : 02_sp_validate_bronze_products.sql
Layer    : Bronze
Purpose  : Checks one Bronze batch against the mandatory rules and copies
           failing rows to bronze_quarantine_bad_records:
             1. URL present (not null, empty or whitespace)
             2. Price parseable after removing all but digits, '.' and '-'
           Bronze rows are never modified.
When     : Deploy at setup and after any change. Runs as step 2 of sp_run_pipeline.
Requires : 01_sp_load_bronze_products has loaded the batch.
Params   : p_batch_id  Bronze batch to validate. NULL = latest loaded batch.
Logs     : bronze_raw_error_logs, bronze_validity_log
Warning  : Re-running for a batch replaces that batch's quarantine rows.
===============================================================================
*/

CREATE OR REPLACE PROCEDURE statement_competitor_data_warehouse.sp_validate_bronze_products (IN p_batch_id STRING)
BEGIN
    DECLARE start_time TIMESTAMP;
    DECLARE v_execution_id STRING;
    DECLARE v_batch_id STRING;
    DECLARE v_error_msg STRING;
    DECLARE v_records_processed INT64;
    DECLARE v_records_failed INT64;
    DECLARE v_records_passed INT64;

    -- Initialise run identifiers.
    SET start_time = CURRENT_TIMESTAMP();
    SET v_execution_id = GENERATE_UUID();

    -- Resolve the batch: the parameter, else the most recently loaded batch.
    SET v_batch_id = COALESCE(
        p_batch_id,
        (
            SELECT batch_id
            FROM statement_competitor_data_warehouse.bronze_raw_products
            WHERE batch_id IS NOT NULL
            ORDER BY ingested_at DESC, batch_id DESC
            LIMIT 1
        )
    );

    -- Stop if there is nothing to validate.
    IF v_batch_id IS NULL THEN
        RAISE USING MESSAGE = 'sp_validate_bronze_products failed: no bronze batch_id found to validate';
    END IF;

    -- Log start.
    INSERT INTO statement_competitor_data_warehouse.bronze_raw_error_logs (
        log_id, execution_id, batch_id, error_timestamp, scraper_job_id, error_type, error_details
    )
    VALUES (
        GENERATE_UUID(), v_execution_id, v_batch_id, CURRENT_TIMESTAMP(),
        'sp_validate_bronze_products', 'INFO_STAGE_START',
        FORMAT('[1/2] VALIDATING bronze batch %s', v_batch_id)
    );

    BEGIN
        -- Remove earlier quarantine rows for this batch so re-runs do not duplicate them.
        DELETE FROM statement_competitor_data_warehouse.bronze_quarantine_bad_records
        WHERE batch_id = v_batch_id;

        -- Quarantine rows that break a mandatory rule.
        -- The price rule must match the Bronze filter in sp_load_silver_products.
        INSERT INTO statement_competitor_data_warehouse.bronze_quarantine_bad_records (
            quarantine_id, execution_id, batch_id, error_reason, quarantine_timestamp, raw_payload
        )
        SELECT
            GENERATE_UUID(),
            v_execution_id,
            v_batch_id,
            CASE
                WHEN NULLIF(TRIM(raw_url), '') IS NULL THEN 'URL is null or empty'
                WHEN SAFE_CAST(REGEXP_REPLACE(raw_price, r'[^0-9.-]', '') AS FLOAT64) IS NULL THEN 'Price not parseable'
            END AS error_reason,
            CURRENT_TIMESTAMP(),
            TO_JSON_STRING(t)
        FROM statement_competitor_data_warehouse.bronze_raw_products AS t
        WHERE batch_id = v_batch_id
          AND (NULLIF(TRIM(raw_url), '') IS NULL OR SAFE_CAST(REGEXP_REPLACE(raw_price, r'[^0-9.-]', '') AS FLOAT64) IS NULL);

        -- Count processed, quarantined and passed rows.
        SET v_records_processed = (
            SELECT COUNT(*)
            FROM statement_competitor_data_warehouse.bronze_raw_products
            WHERE batch_id = v_batch_id
        );

        SET v_records_failed = (
            SELECT COUNT(*)
            FROM statement_competitor_data_warehouse.bronze_quarantine_bad_records
            WHERE batch_id = v_batch_id
        );

        SET v_records_passed = v_records_processed - v_records_failed;

        -- Record the batch result. sp_load_silver_products only loads batches listed here.
        INSERT INTO statement_competitor_data_warehouse.bronze_validity_log (
            log_id, execution_id, batch_id, validation_timestamp, records_processed, records_passed, records_quarantined, duration_seconds
        )
        VALUES (
            GENERATE_UUID(), v_execution_id, v_batch_id, CURRENT_TIMESTAMP(),
            v_records_processed, v_records_passed, v_records_failed,
            TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), start_time, SECOND)
        );

    EXCEPTION WHEN ERROR THEN
        -- Capture the error.
        SET v_error_msg = FORMAT(
            'Message: %s | Statement: %s',
            COALESCE(@@error.message, 'Unknown error occurred'),
            COALESCE(@@error.statement_text, 'Unknown statement')
        );

        -- Log the error.
        INSERT INTO statement_competitor_data_warehouse.bronze_raw_error_logs (
            log_id, execution_id, batch_id, error_timestamp, scraper_job_id, error_type, error_details
        )
        VALUES (
            GENERATE_UUID(), v_execution_id, v_batch_id, CURRENT_TIMESTAMP(),
            'sp_validate_bronze_products', 'ERROR', v_error_msg
        );

        -- Re-raise so the caller sees the failure.
        RAISE USING MESSAGE = FORMAT('sp_validate_bronze_products failed [Execution ID: %s]: %s', v_execution_id, v_error_msg);
    END;

    -- Log completion.
    INSERT INTO statement_competitor_data_warehouse.bronze_raw_error_logs (
        log_id, execution_id, batch_id, error_timestamp, scraper_job_id, error_type, error_details
    )
    VALUES (
        GENERATE_UUID(), v_execution_id, v_batch_id, CURRENT_TIMESTAMP(),
        'sp_validate_bronze_products', 'INFO_STAGE_COMPLETE',
        FORMAT('[2/2] COMPLETED in %d seconds', TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), start_time, SECOND))
    );
END;


/*
===============================================================================
Tests (run manually)
===============================================================================
-- Validate the latest loaded batch.
CALL statement_competitor_data_warehouse.sp_validate_bronze_products(NULL);

-- Validation result per batch (processed = passed + quarantined).
SELECT batch_id, validation_timestamp, records_processed, records_passed, records_quarantined
FROM statement_competitor_data_warehouse.bronze_validity_log
ORDER BY validation_timestamp DESC
LIMIT 10;

-- Quarantine reasons for the latest validated batch.
SELECT error_reason, COUNT(*) AS n, ANY_VALUE(raw_payload) AS example
FROM statement_competitor_data_warehouse.bronze_quarantine_bad_records
WHERE batch_id = (SELECT batch_id FROM statement_competitor_data_warehouse.bronze_validity_log ORDER BY validation_timestamp DESC LIMIT 1)
GROUP BY error_reason;

-- -----------------------------------------------------------------------------
-- Rule test with mock data. Writes to production tables: run all three steps.
-- -----------------------------------------------------------------------------
-- Step 1: insert one passing and three failing rows.
INSERT INTO statement_competitor_data_warehouse.bronze_raw_products
    (raw_shop_name, raw_title, raw_price, raw_url, raw_makers, batch_id, ingested_at)
VALUES
    ('Retro Club', 'Vintage MCM Chair', 'R 2,500.00', 'https://example.com/chair', 'Artecasa', 'TEST_BATCH_001', CURRENT_TIMESTAMP()),     -- passes
    ('Vintage Store', 'Antique Lamp', 'R 1200', NULL, 'Unknown', 'TEST_BATCH_001', CURRENT_TIMESTAMP()),                                 -- missing URL
    ('Cosmic Modern', 'Teak Sideboard', 'Contact for Price', 'https://example.com/sideboard', '', 'TEST_BATCH_001', CURRENT_TIMESTAMP()), -- unparseable price
    ('Modern Living', 'Teak Coffee Table', '   ', '   ', '   ', 'TEST_BATCH_001', CURRENT_TIMESTAMP());                                 -- blank URL and price

-- Step 2: validate and check (expect processed 4, passed 1, quarantined 3).
CALL statement_competitor_data_warehouse.sp_validate_bronze_products('TEST_BATCH_001');

SELECT records_processed, records_passed, records_quarantined
FROM statement_competitor_data_warehouse.bronze_validity_log
WHERE batch_id = 'TEST_BATCH_001';

SELECT error_reason, raw_payload
FROM statement_competitor_data_warehouse.bronze_quarantine_bad_records
WHERE batch_id = 'TEST_BATCH_001';

-- Step 3: remove all test rows (otherwise the Silver load picks up TEST_BATCH_001).
DELETE FROM statement_competitor_data_warehouse.bronze_raw_products WHERE batch_id = 'TEST_BATCH_001';
DELETE FROM statement_competitor_data_warehouse.bronze_quarantine_bad_records WHERE batch_id = 'TEST_BATCH_001';
DELETE FROM statement_competitor_data_warehouse.bronze_validity_log WHERE batch_id = 'TEST_BATCH_001';
DELETE FROM statement_competitor_data_warehouse.bronze_raw_error_logs WHERE batch_id = 'TEST_BATCH_001';
*/
