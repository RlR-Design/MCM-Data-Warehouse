/*
=============================================================================
SILVER LAYER: RECONCILIATION & VALIDATION PROCEDURE

Dataset: statement_competitor_data_warehouse
Procedure: sp_reconciled_silver_products

Description:
This is the final checkpoint in the Silver layer before data is consumed by
the Gold layer. Run it after sp_load_silver_products, sp_validate_silver_products,
sp_normalise_silver_products and sp_load_dim_price_history. It checks:

1. Bronze -> Silver reconciliation: every eligible product URL in the Bronze batch
   used by the latest Silver load is either in Silver or in Silver quarantine.
2. Silver validation: no Silver rows still break the mandatory rules
   (shop name, price present; price 0..999,999.99).
3. Normalisation: every Silver product has a fact row, and every fact row
   points to existing shop, category, maker and material dimension members.
4. Historisation (dim_price_history): every Silver product has exactly one
   current version, it matches Silver's price/availability, and no version
   ends before it starts.
5. Financial sanity: average Silver price is above zero.

Each failed check is logged into `silver_reconciled_error_logs` with a count
and description. The procedure itself only fails on a system error, so a run
with data issues still completes; check the completion log for the summary.
=============================================================================
*/

-- STREAMING_CHUNK: Defining the reconciliation stored procedure...
CREATE OR REPLACE PROCEDURE statement_competitor_data_warehouse.sp_reconciled_silver_products()
OPTIONS(strict_mode=false)
BEGIN
  DECLARE v_execution_id STRING;
  DECLARE v_batch_id STRING;
  DECLARE v_error_msg STRING;
  DECLARE v_bronze_batch_id STRING;
  DECLARE v_checks_run INT64 DEFAULT 0;
  DECLARE v_checks_failed INT64 DEFAULT 0;

  SET v_execution_id = GENERATE_UUID();
  SET v_batch_id = FORMAT_TIMESTAMP('BATCH_REC_%Y%m%d_%H%M%S', CURRENT_TIMESTAMP());

  BEGIN
    -- Log Start
    INSERT INTO statement_competitor_data_warehouse.silver_reconciled_error_logs (
        log_id, execution_id, batch_id, error_timestamp, procedure_name, error_type, error_details
    )
    VALUES (
        GENERATE_UUID(), v_execution_id, v_batch_id, CURRENT_TIMESTAMP(),
        'sp_reconciled_silver_products', 'INFO_STAGE_START', 'Starting Silver Layer Reconciliation'
    );

    -- Bronze batch processed by the most recent Silver load.
    -- Read from its completion log message, so must stay in sync with the
    -- 'COMPLETED for Bronze batch ...' text in sp_load_silver_products.
    SET v_bronze_batch_id = (
        SELECT REGEXP_EXTRACT(error_details, r'for Bronze batch (\S+)\.')
        FROM statement_competitor_data_warehouse.silver_transformed_error_logs
        WHERE scraper_job_id = 'sp_load_silver_products'
          AND error_type = 'INFO_STAGE_COMPLETE'
        ORDER BY error_timestamp DESC
        LIMIT 1
    );

    -- STREAMING_CHUNK: Running all reconciliation checks...
    -- One row per check: failed_count > 0 means the check found issues
    CREATE TEMP TABLE reconciliation_results AS

    -- CHECK 1: Bronze -> Silver. Eligible URLs (same filters as sp_load_silver_products)
    -- that ended up in neither Silver nor Silver quarantine.
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

    -- CHECK 2: Silver rows that still break the mandatory rules (validation not run or failed)
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

    -- CHECK 3a: Silver products with no fact row (normalisation not run)
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

    -- CHECK 3b: Fact rows pointing to missing dimension members
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

    -- CHECK 4a: Silver products with no current price history version
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

    -- CHECK 4b: Products with more than one current version
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

    -- CHECK 4c: Current version does not match Silver (price history not refreshed)
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

    -- CHECK 4d: Versions that end before they start
    SELECT
        'INVALID_HISTORY_INTERVAL',
        COUNT(*),
        FORMAT('%d dim_price_history row(s) have valid_to earlier than valid_from.', COUNT(*))
    FROM statement_competitor_data_warehouse.dim_price_history
    WHERE valid_to < valid_from

    UNION ALL

    -- CHECK 5: Financial sanity
    SELECT
        'FINANCIAL_ANOMALY',
        IF(AVG(price) IS NULL OR AVG(price) <= 0, 1, 0),
        FORMAT('Average Silver price is %s (expected above zero).', COALESCE(CAST(ROUND(AVG(price), 2) AS STRING), 'NULL'))
    FROM statement_competitor_data_warehouse.silver_transformed_products;

    -- STREAMING_CHUNK: Logging failed checks...
    INSERT INTO statement_competitor_data_warehouse.silver_reconciled_error_logs (
        log_id, execution_id, batch_id, error_timestamp, procedure_name, error_type, error_details
    )
    SELECT
        GENERATE_UUID(), v_execution_id, v_batch_id, CURRENT_TIMESTAMP(),
        'sp_reconciled_silver_products', error_type, details
    FROM reconciliation_results
    WHERE failed_count > 0;

    SET v_checks_run = (SELECT COUNT(*) FROM reconciliation_results);
    SET v_checks_failed = (SELECT COUNTIF(failed_count > 0) FROM reconciliation_results);

    -- Log Complete
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
    SET v_error_msg = FORMAT('Message: %s | Statement: %s', COALESCE(@@error.message, 'Unknown error'), COALESCE(@@error.statement_text, 'Unknown statement'));

    INSERT INTO statement_competitor_data_warehouse.silver_reconciled_error_logs (
        log_id, execution_id, batch_id, error_timestamp, procedure_name, error_type, error_details
    ) VALUES (
        GENERATE_UUID(), v_execution_id, v_batch_id, CURRENT_TIMESTAMP(),
        'sp_reconciled_silver_products', 'RECONCILIATION_FAILURE', v_error_msg
    );

    RAISE USING MESSAGE = FORMAT('sp_reconciled_silver_products failed [Execution ID: %s]: %s', v_execution_id, v_error_msg);
  END;
END;

/*
-- =============================================================================
-- INVOCATION & VERIFICATION TESTS
-- =============================================================================

-- 1. Execute the reconciliation procedure
CALL statement_competitor_data_warehouse.sp_reconciled_silver_products();

-- 2. Check the reconciliation error logs to review any flagged data losses or anomalies
SELECT *
FROM statement_competitor_data_warehouse.silver_reconciled_error_logs
ORDER BY error_timestamp DESC
LIMIT 20;
*/
