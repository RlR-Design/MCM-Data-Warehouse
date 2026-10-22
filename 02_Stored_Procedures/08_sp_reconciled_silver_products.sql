/*
===============================================================================
Script   : 08_sp_reconciled_silver_products.sql
Layer    : Silver
Purpose  : Final integrity check before Gold. Each failed check is logged with
           a count and description:
             DATA_DROP_MISMATCH         Bronze URLs in neither Silver nor quarantine
             SCRAPE_FAILURE             Shop scrapes marked FAILED (or health not checked)
             UNVALIDATED_RECORDS        Silver rows still breaking mandatory rules
             MISSING_FACT_ROW           Silver products without a fact row
             ORPHAN_KEY                 Fact rows pointing to missing dimension members
             MISSING_PRICE_HISTORY      Silver products without a current version
             DUPLICATE_CURRENT_VERSION  Products with more than one current version
             PRICE_HISTORY_OUT_OF_DATE  Current version differs from Silver
             INVALID_HISTORY_INTERVAL   Versions ending before they start
             FINANCIAL_ANOMALY          Average Silver price not above zero
When     : Deploy at setup and after any change. Runs as step 8 of sp_run_pipeline.
Requires : Steps 3 to 7 have completed.
Logs     : silver_reconciled_error_logs
Warning  : Data issues are logged, not raised. The procedure fails only on a
           SQL error. Check the completion log for the summary.
===============================================================================
*/

CREATE OR REPLACE PROCEDURE statement_competitor_data_warehouse.sp_reconciled_silver_products()
OPTIONS(strict_mode=false)
BEGIN
    DECLARE v_execution_id STRING;
    DECLARE v_batch_id STRING;
    DECLARE v_error_msg STRING;
    DECLARE v_bronze_batch_id STRING;
    DECLARE v_checks_run INT64 DEFAULT 0;
    DECLARE v_checks_failed INT64 DEFAULT 0;

    -- Initialise run identifiers.
    SET v_execution_id = GENERATE_UUID();
    SET v_batch_id = FORMAT_TIMESTAMP('BATCH_REC_%Y%m%d_%H%M%S', CURRENT_TIMESTAMP());

    BEGIN
        -- Log start.
        INSERT INTO statement_competitor_data_warehouse.silver_reconciled_error_logs (
            log_id, execution_id, batch_id, error_timestamp, procedure_name, error_type, error_details
        )
        VALUES (
            GENERATE_UUID(), v_execution_id, v_batch_id, CURRENT_TIMESTAMP(),
            'sp_reconciled_silver_products', 'INFO_STAGE_START', 'Starting Silver Layer Reconciliation'
        );

        -- Bronze batch of the latest Silver load, read from its completion log text.
        SET v_bronze_batch_id = (
            SELECT REGEXP_EXTRACT(error_details, r'for Bronze batch (\S+)\.')
            FROM statement_competitor_data_warehouse.silver_transformed_error_logs
            WHERE scraper_job_id = 'sp_load_silver_products'
              AND error_type = 'INFO_STAGE_COMPLETE'
            ORDER BY error_timestamp DESC
            LIMIT 1
        );

        -- Run all checks. One row per check; failed_count > 0 means issues found.
        CREATE TEMP TABLE reconciliation_results AS

        -- Bronze -> Silver: eligible Bronze URLs (same filters as the Silver load)
        -- found in neither Silver nor Silver quarantine.
        SELECT
            'DATA_DROP_MISMATCH' AS error_type,
            COUNT(*) AS failed_count,
            FORMAT('%d product URL(s) from Bronze batch %s are in neither Silver nor Silver quarantine.',
                   COUNT(*), COALESCE(v_bronze_batch_id, 'NONE')) AS details
        FROM (
            SELECT DISTINCT TRIM(raw_url) AS url
            FROM statement_competitor_data_warehouse.bronze_raw_products
            WHERE batch_id = v_bronze_batch_id
              AND raw_url IS NOT NULL
              AND TRIM(raw_url) != ''
              AND UPPER(TRIM(raw_url)) NOT IN ('NULL', 'NONE', 'NAN', 'N/A', '-')
              AND SAFE_CAST(REGEXP_REPLACE(raw_price, r'[^0-9.-]', '') AS FLOAT64) IS NOT NULL
        ) b
        WHERE NOT EXISTS (
                SELECT 1 FROM statement_competitor_data_warehouse.silver_transformed_products s
                WHERE s.url = b.url
            )
          AND NOT EXISTS (
                SELECT 1 FROM statement_competitor_data_warehouse.silver_quarantine_bad_records q
                WHERE JSON_VALUE(q.raw_payload, '$.url') = b.url
            )

        UNION ALL

        -- Shop scrapes in the Bronze batch that failed health checks, or no check run.
        SELECT
            'SCRAPE_FAILURE',
            IF(COUNT(*) = 0, 1, COUNTIF(status = 'FAILED')),
            IF(
                COUNT(*) = 0,
                FORMAT('Bronze batch %s has no scrape health results. Run sp_check_scrape_health.', COALESCE(v_bronze_batch_id, 'NONE')),
                FORMAT('%d shop scrape(s) in Bronze batch %s failed health checks: %s.',
                       COUNTIF(status = 'FAILED'), v_bronze_batch_id,
                       COALESCE(STRING_AGG(IF(status = 'FAILED', FORMAT('%s (%s)', shop_name, issues), NULL), ', '), 'none'))
            )
        FROM statement_competitor_data_warehouse.bronze_scrape_health
        WHERE batch_id = v_bronze_batch_id

        UNION ALL

        -- Silver rows that still break the mandatory rules.
        SELECT
            'UNVALIDATED_RECORDS',
            COUNT(*),
            FORMAT('%d Silver row(s) have a missing shop name or price, or a price outside 0..999,999.99. Run sp_validate_silver_products.', COUNT(*))
        FROM statement_competitor_data_warehouse.silver_transformed_products
        WHERE shop_name IS NULL
           OR TRIM(shop_name) = ''
           OR UPPER(TRIM(shop_name)) IN ('NULL', 'NONE', 'NAN', 'N/A', '-', 'UNKNOWN')
           OR price IS NULL
           OR price < 0
           OR price >= 1000000

        UNION ALL

        -- Silver products without a fact row.
        SELECT
            'MISSING_FACT_ROW',
            COUNT(*),
            FORMAT('%d Silver product(s) have no row in fact_product_scrape. Run sp_normalise_silver_products.', COUNT(*))
        FROM statement_competitor_data_warehouse.silver_transformed_products s
        WHERE NOT EXISTS (
            SELECT 1 FROM statement_competitor_data_warehouse.fact_product_scrape f
            WHERE f.url = s.url
        )

        UNION ALL

        -- Fact rows pointing to missing dimension members.
        SELECT
            'ORPHAN_KEY',
            COUNT(*),
            FORMAT('%d fact row(s) reference a missing dimension member (shop: %d, category: %d, maker: %d, material: %d).',
                   COUNT(*), COUNTIF(ds.shop_id IS NULL), COUNTIF(dc.category_id IS NULL),
                   COUNTIF(dm.maker_id IS NULL), COUNTIF(dmat.material_id IS NULL))
        FROM statement_competitor_data_warehouse.fact_product_scrape f
        LEFT JOIN statement_competitor_data_warehouse.dim_shop ds ON ds.shop_id = f.shop_id
        LEFT JOIN statement_competitor_data_warehouse.dim_category dc ON dc.category_id = f.category_id
        LEFT JOIN statement_competitor_data_warehouse.dim_maker dm ON dm.maker_id = f.maker_id
        LEFT JOIN statement_competitor_data_warehouse.dim_material dmat ON dmat.material_id = f.material_id
        WHERE ds.shop_id IS NULL
           OR dc.category_id IS NULL
           OR dm.maker_id IS NULL
           OR dmat.material_id IS NULL

        UNION ALL

        -- Silver products without a current price version.
        SELECT
            'MISSING_PRICE_HISTORY',
            COUNT(*),
            FORMAT('%d Silver product(s) have no current row in dim_price_history. Run sp_load_dim_price_history.', COUNT(*))
        FROM statement_competitor_data_warehouse.silver_transformed_products s
        WHERE NOT EXISTS (
            SELECT 1 FROM statement_competitor_data_warehouse.dim_price_history h
            WHERE h.product_id = FARM_FINGERPRINT(s.url)
              AND h.is_current = TRUE
        )

        UNION ALL

        -- Products with more than one current version.
        SELECT
            'DUPLICATE_CURRENT_VERSION',
            COUNT(*),
            FORMAT('%d product(s) have more than one current row in dim_price_history.', COUNT(*))
        FROM (
            SELECT product_id
            FROM statement_competitor_data_warehouse.dim_price_history
            WHERE is_current = TRUE
            GROUP BY product_id
            HAVING COUNT(*) > 1
        )

        UNION ALL

        -- Current version differs from Silver.
        SELECT
            'PRICE_HISTORY_OUT_OF_DATE',
            COUNT(*),
            FORMAT('%d product(s) have a current price/availability in dim_price_history that differs from Silver. Run sp_load_dim_price_history.', COUNT(*))
        FROM statement_competitor_data_warehouse.silver_transformed_products s
        JOIN statement_competitor_data_warehouse.dim_price_history h
          ON h.product_id = FARM_FINGERPRINT(s.url)
         AND h.is_current = TRUE
        WHERE h.price IS DISTINCT FROM s.price
           OR h.available IS DISTINCT FROM s.available

        UNION ALL

        -- Versions ending before they start.
        SELECT
            'INVALID_HISTORY_INTERVAL',
            COUNT(*),
            FORMAT('%d dim_price_history row(s) have valid_to earlier than valid_from.', COUNT(*))
        FROM statement_competitor_data_warehouse.dim_price_history
        WHERE valid_to < valid_from

        UNION ALL

        -- Average Silver price above zero.
        SELECT
            'FINANCIAL_ANOMALY',
            IF(AVG(price) IS NULL OR AVG(price) <= 0, 1, 0),
            FORMAT('Average Silver price is %s (expected above zero).', COALESCE(CAST(ROUND(AVG(price), 2) AS STRING), 'NULL'))
        FROM statement_competitor_data_warehouse.silver_transformed_products;

        -- Log each failed check.
        INSERT INTO statement_competitor_data_warehouse.silver_reconciled_error_logs (
            log_id, execution_id, batch_id, error_timestamp, procedure_name, error_type, error_details
        )
        SELECT
            GENERATE_UUID(), v_execution_id, v_batch_id, CURRENT_TIMESTAMP(),
            'sp_reconciled_silver_products', error_type, details
        FROM reconciliation_results
        WHERE failed_count > 0;

        -- Count checks run and failed.
        SET v_checks_run = (SELECT COUNT(*) FROM reconciliation_results);
        SET v_checks_failed = (SELECT COUNTIF(failed_count > 0) FROM reconciliation_results);

        -- Log completion with the summary.
        INSERT INTO statement_competitor_data_warehouse.silver_reconciled_error_logs (
            log_id, execution_id, batch_id, error_timestamp, procedure_name, error_type, error_details
        )
        VALUES (
            GENERATE_UUID(), v_execution_id, v_batch_id, CURRENT_TIMESTAMP(),
            'sp_reconciled_silver_products', 'INFO_STAGE_COMPLETE',
            FORMAT('Silver Layer Reconciliation completed: %d of %d checks found issues (Bronze batch checked: %s).',
                   v_checks_failed, v_checks_run, COALESCE(v_bronze_batch_id, 'NONE'))
        );

    EXCEPTION WHEN ERROR THEN
        -- Capture the error.
        SET v_error_msg = FORMAT('Message: %s | Statement: %s', COALESCE(@@error.message, 'Unknown error'), COALESCE(@@error.statement_text, 'Unknown statement'));

        -- Log the error.
        INSERT INTO statement_competitor_data_warehouse.silver_reconciled_error_logs (
            log_id, execution_id, batch_id, error_timestamp, procedure_name, error_type, error_details
        )
        VALUES (
            GENERATE_UUID(), v_execution_id, v_batch_id, CURRENT_TIMESTAMP(),
            'sp_reconciled_silver_products', 'RECONCILIATION_FAILURE', v_error_msg
        );

        -- Re-raise so the caller sees the failure.
        RAISE USING MESSAGE = FORMAT('sp_reconciled_silver_products failed [Execution ID: %s]: %s', v_execution_id, v_error_msg);
    END;
END;


/*
===============================================================================
Tests (run manually)
===============================================================================
-- Run all reconciliation checks.
CALL statement_competitor_data_warehouse.sp_reconciled_silver_products();

-- Latest run: summary and any failed checks.
SELECT error_timestamp, error_type, error_details
FROM statement_competitor_data_warehouse.silver_reconciled_error_logs
WHERE execution_id = (
    SELECT execution_id
    FROM statement_competitor_data_warehouse.silver_reconciled_error_logs
    ORDER BY error_timestamp DESC
    LIMIT 1
)
ORDER BY error_timestamp;

-- Failed checks over time.
SELECT DATE(error_timestamp) AS run_date, error_type, COUNT(*) AS occurrences
FROM statement_competitor_data_warehouse.silver_reconciled_error_logs
WHERE error_type NOT LIKE 'INFO_%'
GROUP BY run_date, error_type
ORDER BY run_date DESC, error_type;
*/
