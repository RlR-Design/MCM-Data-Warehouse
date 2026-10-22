/*
=============================================================================
SILVER LAYER: HISTORISATION STORED PROCEDURE (SCD Type 2)
Dataset: statement_competitor_data_warehouse
Procedure: sp_load_dim_price_history
Description:
This stored procedure reads data from silver_transformed_products and maintains
a Type 2 Slowly Changing Dimension (SCD2) for pricing and availability history.

Logic:
1. Stages every product that is new or whose price or availability has changed,
   with one effective time per change: the scrape time (or the run time if the
   scrape time is unknown), never earlier than the version it replaces.
2. Expires the old version (valid_to = effective time, is_current = FALSE).
3. Inserts the new version (valid_from = the same effective time, so there are
   no gaps or overlaps; valid_to = '9999-12-31'; is_current = TRUE).
   Steps 2 and 3 run in one transaction: either both happen or neither does.
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
        log_id, execution_id, batch_id, error_timestamp, procedure_name, error_type, error_details
    )
    VALUES (
        GENERATE_UUID(), v_execution_id, v_batch_id, CURRENT_TIMESTAMP(),
        'sp_load_dim_price_history', 'INFO_STAGE_START',
        'Starting SCD Type 2 Historisation for dim_price_history.'
    );

    BEGIN
        BEGIN TRANSACTION;

        -- =============================================================================
        -- STEP 1: STAGE CHANGES
        -- New products, or products whose price or availability differs from the
        -- current version. IS DISTINCT FROM treats NULL as a value, so changes
        -- to/from unknown availability are tracked.
        -- =============================================================================
        CREATE TEMP TABLE stg_price_changes AS
        SELECT
            FARM_FINGERPRINT(source.url) AS product_id,
            CAST(DATETIME(source.timestamp_date, source.timestamp_time) AS TIMESTAMP) AS scrape_timestamp,
            source.price,
            source.available,
            -- The change takes effect at the scrape time (run time if unknown),
            -- but never before the version it replaces, so intervals cannot overlap
            GREATEST(
                COALESCE(CAST(DATETIME(source.timestamp_date, source.timestamp_time) AS TIMESTAMP), start_time),
                COALESCE(current_version.valid_from, TIMESTAMP '0001-01-01 00:00:00 UTC')
            ) AS effective_from,
            current_version.product_id IS NOT NULL AS has_current_version
        FROM `statement_competitor_data_warehouse.silver_transformed_products` AS source
        LEFT JOIN `statement_competitor_data_warehouse.dim_price_history` AS current_version
          ON current_version.product_id = FARM_FINGERPRINT(source.url)
          AND current_version.is_current = TRUE
        WHERE current_version.product_id IS NULL
           OR current_version.price IS DISTINCT FROM source.price
           OR current_version.available IS DISTINCT FROM source.available;

        -- =============================================================================
        -- STEP 2: EXPIRE OLD VERSIONS (UPDATE)
        -- valid_to is set to the exact valid_from of the new version
        -- =============================================================================
        UPDATE `statement_competitor_data_warehouse.dim_price_history` AS target
        SET
            valid_to = stg.effective_from,
            is_current = FALSE,
            execution_id = v_execution_id,
            batch_id = v_batch_id
        FROM stg_price_changes AS stg
        WHERE target.product_id = stg.product_id
          AND target.is_current = TRUE
          AND stg.has_current_version;

        SET v_records_expired = @@row_count;

        -- =============================================================================
        -- STEP 3: INSERT NEW VERSIONS
        -- Completely new products and new versions of the products just expired
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
            batch_id,
            ingested_at
        )
        SELECT
            FARM_FINGERPRINT(GENERATE_UUID()) AS price_log_id,
            product_id,
            scrape_timestamp,
            price,
            available,
            effective_from AS valid_from,
            TIMESTAMP('9999-12-31 23:59:59 UTC') AS valid_to,
            TRUE AS is_current,
            v_execution_id AS execution_id,
            v_batch_id AS batch_id,
            CAST(start_time AS DATETIME) AS ingested_at
        FROM stg_price_changes;

        SET v_records_inserted = @@row_count;

        COMMIT TRANSACTION;

    EXCEPTION WHEN ERROR THEN
        -- =============================================================================
        -- STEP 4: ERROR HANDLING
        -- Undo any partial expire/insert, then log (the log is written after the
        -- rollback so it is kept)
        -- =============================================================================
        ROLLBACK TRANSACTION;

        SET v_error_msg = FORMAT('Message: %s | Statement: %s', COALESCE(@@error.message, 'Unknown error'), COALESCE(@@error.statement_text, 'Unknown statement'));
        SET v_error_type = 'HISTORISATION_FAILURE';

        INSERT INTO statement_competitor_data_warehouse.silver_historisation_error_logs (
            log_id, execution_id, batch_id, error_timestamp, procedure_name, error_type, error_details
        )
        VALUES (
            GENERATE_UUID(), v_execution_id, v_batch_id, CURRENT_TIMESTAMP(),
            'sp_load_dim_price_history', v_error_type, v_error_msg
        );

        RAISE USING MESSAGE = FORMAT('sp_load_dim_price_history failed [Execution ID: %s]: %s', v_execution_id, v_error_msg);
    END;

    -- =============================================================================
    -- STEP 5: LOG SUCCESS
    -- =============================================================================
    INSERT INTO statement_competitor_data_warehouse.silver_historisation_error_logs (
        log_id, execution_id, batch_id, error_timestamp, procedure_name, error_type, error_details
    )
    VALUES (
        GENERATE_UUID(), v_execution_id, v_batch_id, CURRENT_TIMESTAMP(),
        'sp_load_dim_price_history', 'INFO_STAGE_COMPLETE',
        FORMAT('SCD2 Historisation COMPLETED. Records Expired: %d, Records Inserted: %d in %d seconds',
               v_records_expired, v_records_inserted, TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), start_time, SECOND))
    );
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
