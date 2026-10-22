/*
=============================================================================
GOLD LAYER: HISTORISATION STORED PROCEDURE (SCD Type 2)
Dataset: statement_competitor_data_warehouse
Procedure: sp_load_dim_price_history
Description:
This stored procedure reads data from silver_transformed_products and maintains
a Type 2 Slowly Changing Dimension (SCD2) for pricing and availability history.

Logic:
1. Identifies existing active records where price or availability has changed.
2. Expires old records (sets valid_to = CURRENT_TIMESTAMP, is_current = FALSE).
3. Inserts new records with updated attributes and a valid_to of '9999-12-31'
   (sets is_current = TRUE).
4. Logs execution and error events to silver_historisation_error_logs.
=============================================================================
*/

CREATE OR REPLACE PROCEDURE statement_competitor_data_warehouse.sp_load_dim_price_history()
OPTIONS(strict_mode=false)
BEGIN
    -- Declare auditing and execution variables
    DECLARE start_time TIMESTAMP;
    DECLARE v_execution_id STRING;
    DECLARE v_batch_id STRING;
    DECLARE v_error_msg STRING;
    DECLARE v_error_type STRING;

    DECLARE v_records_expired INT64 DEFAULT 0;
    DECLARE v_records_inserted INT64 DEFAULT 0;

    SET start_time = CURRENT_TIMESTAMP();
    SET v_execution_id = GENERATE_UUID();
    SET v_batch_id = FORMAT_TIMESTAMP('BATCH_HIST_%Y%m%d_%H%M%S', start_time);

    -- Log start of procedure
    INSERT INTO statement_competitor_data_warehouse.silver_historisation_error_logs (
        log_id, execution_id, batch_id, error_timestamp, error_type, error_details
    )
    VALUES (
        GENERATE_UUID(), v_execution_id, v_batch_id, CURRENT_TIMESTAMP(),
        'INFO_STAGE_START',
        'Starting SCD Type 2 Historisation for dim_price_history.'
    );

    BEGIN
        -- =============================================================================
        -- STEP 1: EXPIRE OLD RECORDS (UPDATE)
        -- Expire existing active records if the price or availability has changed
        -- =============================================================================
        UPDATE `statement_competitor_data_warehouse.dim_price_history` AS target
        SET
            valid_to = CURRENT_TIMESTAMP(),
            is_current = FALSE,
            execution_id = v_execution_id,
            batch_id = v_batch_id
        FROM `statement_competitor_data_warehouse.silver_transformed_products` AS source
        WHERE target.product_id = FARM_FINGERPRINT(source.url)
          AND target.is_current = TRUE
          -- IS DISTINCT FROM treats NULL as a value, so changes to/from unknown availability are tracked
          AND (target.price IS DISTINCT FROM source.price OR target.available IS DISTINCT FROM source.available);

        SET v_records_expired = @@row_count;

        -- =============================================================================
        -- STEP 2: INSERT NEW RECORDS
        -- Insert completely new products OR new versions of the products we just expired
        -- =============================================================================
        INSERT INTO `statement_competitor_data_warehouse.dim_price_history` (
            price_log_id,
            product_id,
            scrape_timestamp,
            price,
            available,
            valid_from,
            valid_to,
            is_current,
            execution_id,
            batch_id
        )
        SELECT
            FARM_FINGERPRINT(GENERATE_UUID()) AS price_log_id,
            FARM_FINGERPRINT(source.url) AS product_id,
            CAST(DATETIME(source.timestamp_date, source.timestamp_time) AS TIMESTAMP) AS scrape_timestamp,
            source.price,
            source.available,
            CURRENT_TIMESTAMP() AS valid_from,
            TIMESTAMP('9999-12-31 23:59:59 UTC') AS valid_to,
            TRUE AS is_current,
            v_execution_id AS execution_id,
            v_batch_id AS batch_id
        FROM `statement_competitor_data_warehouse.silver_transformed_products` AS source
        -- A LEFT JOIN on currently active records filters out items that haven't changed.
        -- Items that were just expired in Step 1 will have no active record here,
        -- evaluating to NULL and causing them to be successfully inserted.
        LEFT JOIN `statement_competitor_data_warehouse.dim_price_history` AS target
          ON FARM_FINGERPRINT(source.url) = target.product_id
          AND target.is_current = TRUE
        WHERE target.product_id IS NULL;

        SET v_records_inserted = @@row_count;

        -- =============================================================================
        -- STEP 3: LOG SUCCESS
        -- =============================================================================
        INSERT INTO statement_competitor_data_warehouse.silver_historisation_error_logs (
            log_id, execution_id, batch_id, error_timestamp, error_type, error_details
        )
        VALUES (
            GENERATE_UUID(), v_execution_id, v_batch_id, CURRENT_TIMESTAMP(),
            'INFO_STAGE_COMPLETE',
            FORMAT('SCD2 Historisation COMPLETED. Records Expired: %d, Records Inserted: %d in %d seconds',
                   v_records_expired, v_records_inserted, TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), start_time, SECOND))
        );

    EXCEPTION WHEN ERROR THEN
        -- =============================================================================
        -- STEP 4: ERROR HANDLING
        -- =============================================================================
        SET v_error_msg = FORMAT('Message: %s | Statement: %s', COALESCE(@@error.message, 'Unknown error'), COALESCE(@@error.statement_text, 'Unknown statement'));
        SET v_error_type = 'HISTORISATION_FAILURE';

        INSERT INTO statement_competitor_data_warehouse.silver_historisation_error_logs (
            log_id, execution_id, batch_id, error_timestamp, error_type, error_details
        )
        VALUES (
            GENERATE_UUID(), v_execution_id, v_batch_id, CURRENT_TIMESTAMP(),
            v_error_type, v_error_msg
        );

        RAISE USING MESSAGE = FORMAT('sp_load_dim_price_history failed [Execution ID: %s]: %s', v_execution_id, v_error_msg);
    END;
END;

/*
-- =============================================================================
-- INVOCATION & VERIFICATION TESTS
-- =============================================================================

-- 1. Execute the stored procedure
CALL statement_competitor_data_warehouse.sp_load_dim_price_history();

-- 2. Verify total records vs active records
SELECT
    COUNT(*) AS total_history_records,
    COUNTIF(is_current = TRUE) AS active_records,
    COUNTIF(is_current = FALSE) AS expired_records
FROM statement_competitor_data_warehouse.dim_price_history;

-- 3. Check for products that have changed (SCD2 demonstration)
SELECT
    product_id,
    COUNT(*) as version_count,
    STRING_AGG(CAST(price AS STRING), ' -> ' ORDER BY valid_from) as price_changes,
    STRING_AGG(CAST(available AS STRING), ' -> ' ORDER BY valid_from) as availability_changes
FROM statement_competitor_data_warehouse.dim_price_history
GROUP BY product_id
HAVING version_count > 1
ORDER BY version_count DESC;

-- 4. Review recent execution logs and captured errors
SELECT *
FROM statement_competitor_data_warehouse.silver_historisation_error_logs
ORDER BY error_timestamp DESC
LIMIT 10;
*/