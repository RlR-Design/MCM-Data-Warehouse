/*
===============================================================================
Script   : 07_sp_load_dim_price_history.sql
Layer    : Silver
Purpose  : Maintains dim_price_history as an SCD Type 2 table: one row per
           price/availability version per product. Changed products get their
           current version expired and a new version inserted, in one
           transaction, with no gaps between versions.
When     : Deploy at setup and after any change. Runs as step 7 of sp_run_pipeline.
Requires : 05_sp_validate_silver_products has completed.
Logs     : silver_historisation_error_logs
Warning  : History cannot be rebuilt. Never truncate or recreate dim_price_history.
===============================================================================
*/

CREATE OR REPLACE PROCEDURE statement_competitor_data_warehouse.sp_load_dim_price_history()
OPTIONS(strict_mode=false)
BEGIN
    DECLARE start_time TIMESTAMP;
    DECLARE v_execution_id STRING;
    DECLARE v_batch_id STRING;
    DECLARE v_error_msg STRING;
    DECLARE v_error_type STRING;
    DECLARE v_records_expired INT64 DEFAULT 0;
    DECLARE v_records_inserted INT64 DEFAULT 0;

    -- Initialise run identifiers.
    SET start_time = CURRENT_TIMESTAMP();
    SET v_execution_id = GENERATE_UUID();
    SET v_batch_id = FORMAT_TIMESTAMP('BATCH_HIST_%Y%m%d_%H%M%S', start_time);

    -- Log start.
    INSERT INTO statement_competitor_data_warehouse.silver_historisation_error_logs (
        log_id, execution_id, batch_id, error_timestamp, procedure_name, error_type, error_details
    )
    VALUES (
        GENERATE_UUID(), v_execution_id, v_batch_id, CURRENT_TIMESTAMP(),
        'sp_load_dim_price_history', 'INFO_STAGE_START',
        'Starting SCD Type 2 Historisation for dim_price_history.'
    );

    BEGIN
        -- Expire and insert are one transaction: both happen or neither does.
        BEGIN TRANSACTION;

        -- Stage new products and products whose price or availability changed
        -- (IS DISTINCT FROM also detects changes to and from NULL).
        CREATE TEMP TABLE stg_price_changes AS
        SELECT
            FARM_FINGERPRINT(source.url) AS product_id,
            CAST(DATETIME(source.timestamp_date, source.timestamp_time) AS TIMESTAMP) AS scrape_timestamp,
            source.price,
            source.available,
            -- Effective time: scrape time (run time if unknown), never before the current version.
            GREATEST(
                COALESCE(CAST(DATETIME(source.timestamp_date, source.timestamp_time) AS TIMESTAMP), start_time),
                COALESCE(current_version.valid_from, TIMESTAMP '0001-01-01 00:00:00 UTC')
            ) AS effective_from,
            current_version.product_id IS NOT NULL AS has_current_version
        FROM statement_competitor_data_warehouse.silver_transformed_products AS source
        LEFT JOIN statement_competitor_data_warehouse.dim_price_history AS current_version
          ON current_version.product_id = FARM_FINGERPRINT(source.url)
          AND current_version.is_current = TRUE
        WHERE current_version.product_id IS NULL
           OR current_version.price IS DISTINCT FROM source.price
           OR current_version.available IS DISTINCT FROM source.available;

        -- Expire current versions that changed (valid_to = new version's valid_from).
        UPDATE statement_competitor_data_warehouse.dim_price_history AS target
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

        -- Insert the new versions.
        INSERT INTO statement_competitor_data_warehouse.dim_price_history (
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
        -- Undo the partial expire/insert (log is written after the rollback so it is kept).
        ROLLBACK TRANSACTION;

        -- Capture the error.
        SET v_error_msg = FORMAT('Message: %s | Statement: %s', COALESCE(@@error.message, 'Unknown error'), COALESCE(@@error.statement_text, 'Unknown statement'));
        SET v_error_type = 'HISTORISATION_FAILURE';

        -- Log the error.
        INSERT INTO statement_competitor_data_warehouse.silver_historisation_error_logs (
            log_id, execution_id, batch_id, error_timestamp, procedure_name, error_type, error_details
        )
        VALUES (
            GENERATE_UUID(), v_execution_id, v_batch_id, CURRENT_TIMESTAMP(),
            'sp_load_dim_price_history', v_error_type, v_error_msg
        );

        -- Re-raise so the caller sees the failure.
        RAISE USING MESSAGE = FORMAT('sp_load_dim_price_history failed [Execution ID: %s]: %s', v_execution_id, v_error_msg);
    END;

    -- Log completion.
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
===============================================================================
Tests (run manually)
===============================================================================
-- Update price history from Silver.
CALL statement_competitor_data_warehouse.sp_load_dim_price_history();

-- Latest run result (records expired and inserted).
SELECT error_timestamp, error_type, error_details
FROM statement_competitor_data_warehouse.silver_historisation_error_logs
ORDER BY error_timestamp DESC
LIMIT 5;

-- Total, current and expired versions.
SELECT
    COUNT(*) AS total_versions,
    COUNTIF(is_current) AS current_versions,
    COUNTIF(NOT is_current) AS expired_versions
FROM statement_competitor_data_warehouse.dim_price_history;

-- One current version per product (expect no rows).
SELECT product_id, COUNT(*) AS n
FROM statement_competitor_data_warehouse.dim_price_history
WHERE is_current
GROUP BY product_id
HAVING COUNT(*) > 1;

-- No gaps or overlaps between versions (expect no rows; versions written before 2026-10-22 may show small gaps).
SELECT product_id, valid_to, next_valid_from
FROM (
    SELECT product_id, valid_to, LEAD(valid_from) OVER (PARTITION BY product_id ORDER BY valid_from) AS next_valid_from
    FROM statement_competitor_data_warehouse.dim_price_history
)
WHERE next_valid_from IS NOT NULL
  AND valid_to != next_valid_from;

-- Products with price or availability changes.
SELECT
    product_id,
    COUNT(*) AS versions,
    STRING_AGG(CAST(price AS STRING), ' -> ' ORDER BY valid_from) AS price_changes,
    STRING_AGG(CAST(available AS STRING), ' -> ' ORDER BY valid_from) AS availability_changes
FROM statement_competitor_data_warehouse.dim_price_history
GROUP BY product_id
HAVING COUNT(*) > 1
ORDER BY versions DESC;
*/
