/*
===============================================================================
Script   : 03_sp_check_scrape_health.sql
Layer    : Bronze
Purpose  : Checks each shop's scrape in one Bronze batch against that shop's
           last 3 non-failed scrapes and records the result in
           bronze_scrape_health:
             FAILED   SHOP_MISSING          shop from recent batches has no rows
                      LOW_PRODUCT_COUNT     products below min_count_ratio x baseline
                      HIGH_QUARANTINE_RATE  quarantined share above max_quarantine_rate
                      STALE_SCRAPE          latest scrape time not newer than before
             WARNING  FIELD_DROPOUT         normally filled field mostly empty
                      DUPLICATE_URLS        repeated URLs above max_duplicate_rate
           FAILED scrapes are still loaded; Gold ignores them for delisting.
When     : Deploy at setup and after any change. Runs as step 3 of sp_run_pipeline.
Requires : 02_sp_validate_bronze_products has validated the batch.
           Thresholds in ref_shop_rules (04_create_reference_tables.sql).
Params   : p_batch_id  Bronze batch to check. NULL = latest validated batch.
Logs     : bronze_raw_error_logs (one SCRAPE_FAILED / SCRAPE_WARNING row per shop issue)
Warning  : Never stops the pipeline for data issues. The first batch has no
           history, so only the quarantine and duplicate checks apply.
===============================================================================
*/

CREATE OR REPLACE PROCEDURE statement_competitor_data_warehouse.sp_check_scrape_health (IN p_batch_id STRING)
OPTIONS(strict_mode=false)
BEGIN
    DECLARE start_time TIMESTAMP;
    DECLARE v_execution_id STRING;
    DECLARE v_batch_id STRING;
    DECLARE v_batch_ingested_at TIMESTAMP;
    DECLARE v_error_msg STRING;
    DECLARE v_shops_healthy INT64 DEFAULT 0;
    DECLARE v_shops_warning INT64 DEFAULT 0;
    DECLARE v_shops_failed INT64 DEFAULT 0;

    -- Initialise run identifiers.
    SET start_time = CURRENT_TIMESTAMP();
    SET v_execution_id = GENERATE_UUID();

    -- Resolve the batch: the parameter, else the latest validated batch.
    SET v_batch_id = COALESCE(
        p_batch_id,
        (
            SELECT batch_id
            FROM statement_competitor_data_warehouse.bronze_validity_log
            ORDER BY validation_timestamp DESC
            LIMIT 1
        )
    );

    -- Log start.
    INSERT INTO statement_competitor_data_warehouse.bronze_raw_error_logs (
        log_id, execution_id, batch_id, error_timestamp, scraper_job_id, error_type, error_details
    )
    VALUES (
        GENERATE_UUID(), v_execution_id, v_batch_id, CURRENT_TIMESTAMP(),
        'sp_check_scrape_health', 'INFO_STAGE_START',
        FORMAT('Checking scrape health for Bronze batch %s', COALESCE(v_batch_id, 'NONE'))
    );

    BEGIN
        -- Stop unless the batch has been through Bronze validation (quarantine counts are needed).
        IF v_batch_id IS NULL OR NOT EXISTS (
            SELECT 1
            FROM statement_competitor_data_warehouse.bronze_validity_log
            WHERE batch_id = v_batch_id
        ) THEN
            RAISE USING MESSAGE = FORMAT('Bronze batch %s has not been validated. Run sp_validate_bronze_products first.', COALESCE(v_batch_id, 'NONE'));
        END IF;

        -- Load time of the batch, used to find earlier batches.
        SET v_batch_ingested_at = (
            SELECT MIN(ingested_at)
            FROM statement_competitor_data_warehouse.bronze_raw_products
            WHERE batch_id = v_batch_id
        );

        -- Remove earlier results for this batch so re-runs do not duplicate them.
        DELETE FROM statement_competitor_data_warehouse.bronze_scrape_health
        WHERE batch_id = v_batch_id;

        -- Per-shop statistics for every batch (test batches excluded).
        CREATE TEMP TABLE stg_shop_batch_stats AS
        SELECT
            batch_id,
            TRIM(raw_shop_name) AS shop_name,
            MIN(ingested_at) AS ingested_at,
            COUNT(*) AS row_count,
            COUNTIF(NULLIF(TRIM(raw_url), '') IS NOT NULL) AS rows_with_url,
            COUNT(DISTINCT NULLIF(TRIM(raw_url), '')) AS product_count,
            SAFE_DIVIDE(COUNTIF(NULLIF(TRIM(raw_title), '') IS NULL), COUNT(*)) AS title_empty_rate,
            SAFE_DIVIDE(COUNTIF(NULLIF(TRIM(raw_price), '') IS NULL), COUNT(*)) AS price_empty_rate,
            SAFE_DIVIDE(COUNTIF(NULLIF(TRIM(raw_available), '') IS NULL), COUNT(*)) AS available_empty_rate,
            SAFE_DIVIDE(COUNTIF(NULLIF(TRIM(scrape_timestamp), '') IS NULL), COUNT(*)) AS scrape_ts_empty_rate,
            -- Same timestamp formats as sp_load_silver_products.
            MAX(COALESCE(
                SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%E*S', TRIM(scrape_timestamp)),
                SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*S', TRIM(scrape_timestamp)),
                SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%SZ', TRIM(scrape_timestamp)),
                SAFE_CAST(TRIM(scrape_timestamp) AS TIMESTAMP)
            )) AS latest_scrape_ts
        FROM statement_competitor_data_warehouse.bronze_raw_products
        WHERE batch_id IS NOT NULL
          AND NOT STARTS_WITH(batch_id, 'TEST_')
          AND NULLIF(TRIM(raw_shop_name), '') IS NOT NULL
          AND UPPER(TRIM(raw_shop_name)) NOT IN ('NULL', 'NONE', 'NAN', 'N/A', '-', 'UNKNOWN')
        GROUP BY batch_id, shop_name;

        -- Evaluate every shop in this batch or in the 3 previous batches.
        CREATE TEMP TABLE stg_scrape_health AS
        WITH current_batch AS (
            SELECT *
            FROM stg_shop_batch_stats
            WHERE batch_id = v_batch_id
        ),
        -- The 3 batches loaded before this one.
        previous_batches AS (
            SELECT batch_id
            FROM stg_shop_batch_stats
            WHERE ingested_at < v_batch_ingested_at
            GROUP BY batch_id
            QUALIFY ROW_NUMBER() OVER (ORDER BY MIN(ingested_at) DESC) <= 3
        ),
        -- Shops expected in this batch: present in any of the 3 previous batches.
        expected_shops AS (
            SELECT DISTINCT s.shop_name
            FROM stg_shop_batch_stats s
            JOIN previous_batches pb ON pb.batch_id = s.batch_id
        ),
        -- Each shop's last 3 earlier scrapes that were not marked FAILED.
        prior_scrapes AS (
            SELECT s.*
            FROM stg_shop_batch_stats s
            WHERE s.ingested_at < v_batch_ingested_at
              AND NOT EXISTS (
                  SELECT 1
                  FROM statement_competitor_data_warehouse.bronze_scrape_health h
                  WHERE h.batch_id = s.batch_id
                    AND h.shop_name = s.shop_name
                    AND h.status = 'FAILED'
              )
            QUALIFY ROW_NUMBER() OVER (PARTITION BY s.shop_name ORDER BY s.ingested_at DESC) <= 3
        ),
        -- Baseline per shop from those scrapes.
        baseline AS (
            SELECT
                shop_name,
                APPROX_QUANTILES(product_count, 2)[OFFSET(1)] AS baseline_count,
                AVG(title_empty_rate) AS title_empty_rate,
                AVG(price_empty_rate) AS price_empty_rate,
                AVG(available_empty_rate) AS available_empty_rate,
                AVG(scrape_ts_empty_rate) AS scrape_ts_empty_rate,
                MAX(latest_scrape_ts) AS previous_scrape_ts
            FROM prior_scrapes
            GROUP BY shop_name
        ),
        -- Quarantined rows per shop in this batch.
        quarantine AS (
            SELECT
                TRIM(JSON_VALUE(raw_payload, '$.raw_shop_name')) AS shop_name,
                COUNT(*) AS quarantined
            FROM statement_competitor_data_warehouse.bronze_quarantine_bad_records
            WHERE batch_id = v_batch_id
            GROUP BY shop_name
        ),
        shops AS (
            SELECT shop_name FROM current_batch
            UNION DISTINCT
            SELECT shop_name FROM expected_shops
        ),
        -- Measurements and thresholds per shop (shop rule, else '*' default, else built-in).
        measured AS (
            SELECT
                sh.shop_name,
                c.product_count,
                b.baseline_count,
                SAFE_DIVIDE(COALESCE(q.quarantined, 0), c.row_count) AS quarantine_rate,
                1 - SAFE_DIVIDE(c.product_count, c.rows_with_url) AS duplicate_url_rate,
                c.latest_scrape_ts,
                b.previous_scrape_ts,
                c.title_empty_rate, b.title_empty_rate AS base_title_empty_rate,
                c.price_empty_rate, b.price_empty_rate AS base_price_empty_rate,
                c.available_empty_rate, b.available_empty_rate AS base_available_empty_rate,
                c.scrape_ts_empty_rate, b.scrape_ts_empty_rate AS base_scrape_ts_empty_rate,
                COALESCE(sr.min_count_ratio, dr.min_count_ratio, 0.7) AS min_count_ratio,
                COALESCE(sr.max_quarantine_rate, dr.max_quarantine_rate, 0.2) AS max_quarantine_rate,
                COALESCE(sr.max_duplicate_rate, dr.max_duplicate_rate, 0.05) AS max_duplicate_rate,
                COALESCE(sr.max_field_empty_rate, dr.max_field_empty_rate, 0.5) AS max_field_empty_rate
            FROM shops sh
            LEFT JOIN current_batch c ON c.shop_name = sh.shop_name
            LEFT JOIN baseline b ON b.shop_name = sh.shop_name
            LEFT JOIN quarantine q ON q.shop_name = sh.shop_name
            LEFT JOIN statement_competitor_data_warehouse.ref_shop_rules sr ON LOWER(sr.shop_name) = LOWER(sh.shop_name)
            LEFT JOIN statement_competitor_data_warehouse.ref_shop_rules dr ON dr.shop_name = '*'
        ),
        -- Apply each check. A field counts as normally filled when under 10% empty in the baseline.
        flagged AS (
            SELECT
                *,
                product_count IS NULL AS is_shop_missing,
                COALESCE(product_count < baseline_count * min_count_ratio, FALSE) AS is_low_count,
                COALESCE(quarantine_rate > max_quarantine_rate, FALSE) AS is_high_quarantine,
                COALESCE(latest_scrape_ts <= previous_scrape_ts, FALSE) AS is_stale,
                COALESCE(
                       (title_empty_rate > max_field_empty_rate AND base_title_empty_rate < 0.1)
                    OR (price_empty_rate > max_field_empty_rate AND base_price_empty_rate < 0.1)
                    OR (available_empty_rate > max_field_empty_rate AND base_available_empty_rate < 0.1)
                    OR (scrape_ts_empty_rate > max_field_empty_rate AND base_scrape_ts_empty_rate < 0.1),
                    FALSE
                ) AS is_field_dropout,
                COALESCE(duplicate_url_rate > max_duplicate_rate, FALSE) AS is_duplicate_urls
            FROM measured
        )
        SELECT
            shop_name,
            CASE
                WHEN is_shop_missing OR is_low_count OR is_high_quarantine OR is_stale THEN 'FAILED'
                WHEN is_field_dropout OR is_duplicate_urls THEN 'WARNING'
                ELSE 'HEALTHY'
            END AS status,
            ARRAY_TO_STRING([
                IF(is_shop_missing, 'SHOP_MISSING', NULL),
                IF(is_low_count, 'LOW_PRODUCT_COUNT', NULL),
                IF(is_high_quarantine, 'HIGH_QUARANTINE_RATE', NULL),
                IF(is_stale, 'STALE_SCRAPE', NULL),
                IF(is_field_dropout, 'FIELD_DROPOUT', NULL),
                IF(is_duplicate_urls, 'DUPLICATE_URLS', NULL)
            ], '; ') AS issues,
            product_count,
            baseline_count,
            quarantine_rate,
            duplicate_url_rate,
            latest_scrape_ts,
            FORMAT(
                'products %s (baseline %s); quarantined %s%%; duplicate URLs %s%%; empty title/price/available/scrape time %s/%s/%s/%s%%; latest scrape %s (previous %s)',
                COALESCE(CAST(product_count AS STRING), '-'),
                COALESCE(CAST(baseline_count AS STRING), '-'),
                COALESCE(CAST(ROUND(quarantine_rate * 100, 1) AS STRING), '-'),
                COALESCE(CAST(ROUND(duplicate_url_rate * 100, 1) AS STRING), '-'),
                COALESCE(CAST(ROUND(title_empty_rate * 100) AS STRING), '-'),
                COALESCE(CAST(ROUND(price_empty_rate * 100) AS STRING), '-'),
                COALESCE(CAST(ROUND(available_empty_rate * 100) AS STRING), '-'),
                COALESCE(CAST(ROUND(scrape_ts_empty_rate * 100) AS STRING), '-'),
                COALESCE(CAST(latest_scrape_ts AS STRING), '-'),
                COALESCE(CAST(previous_scrape_ts AS STRING), '-')
            ) AS details
        FROM flagged;

        -- Record the result per shop.
        INSERT INTO statement_competitor_data_warehouse.bronze_scrape_health (
            health_id, execution_id, batch_id, shop_name, checked_at, status, issues,
            product_count, baseline_count, quarantine_rate, duplicate_url_rate, latest_scrape_ts, details
        )
        SELECT
            GENERATE_UUID(), v_execution_id, v_batch_id, shop_name, CURRENT_TIMESTAMP(), status, issues,
            product_count, baseline_count, quarantine_rate, duplicate_url_rate, latest_scrape_ts, details
        FROM stg_scrape_health;

        -- Log one row per shop with issues.
        INSERT INTO statement_competitor_data_warehouse.bronze_raw_error_logs (
            log_id, execution_id, batch_id, error_timestamp, scraper_job_id, error_type, error_details
        )
        SELECT
            GENERATE_UUID(), v_execution_id, v_batch_id, CURRENT_TIMESTAMP(),
            'sp_check_scrape_health', CONCAT('SCRAPE_', status),
            FORMAT('%s: %s | %s', shop_name, issues, details)
        FROM stg_scrape_health
        WHERE status != 'HEALTHY';

        -- Count shops per status.
        SET v_shops_healthy = (SELECT COUNTIF(status = 'HEALTHY') FROM stg_scrape_health);
        SET v_shops_warning = (SELECT COUNTIF(status = 'WARNING') FROM stg_scrape_health);
        SET v_shops_failed = (SELECT COUNTIF(status = 'FAILED') FROM stg_scrape_health);

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
            'sp_check_scrape_health', 'ERROR', v_error_msg
        );

        -- Re-raise so the caller sees the failure.
        RAISE USING MESSAGE = FORMAT('sp_check_scrape_health failed [Execution ID: %s]: %s', v_execution_id, v_error_msg);
    END;

    -- Log completion with the summary.
    INSERT INTO statement_competitor_data_warehouse.bronze_raw_error_logs (
        log_id, execution_id, batch_id, error_timestamp, scraper_job_id, error_type, error_details
    )
    VALUES (
        GENERATE_UUID(), v_execution_id, v_batch_id, CURRENT_TIMESTAMP(),
        'sp_check_scrape_health', 'INFO_STAGE_COMPLETE',
        FORMAT('Scrape health COMPLETED: %d healthy, %d warning, %d failed in %d seconds',
               v_shops_healthy, v_shops_warning, v_shops_failed, TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), start_time, SECOND))
    );
END;


/*
===============================================================================
Tests (run manually)
===============================================================================
-- Check the latest validated batch.
CALL statement_competitor_data_warehouse.sp_check_scrape_health(NULL);

-- Check a specific batch.
CALL statement_competitor_data_warehouse.sp_check_scrape_health('BATCH_YYYYMMDD_HHMMSS');

-- Result per shop for the latest checked batch.
SELECT shop_name, status, issues, product_count, baseline_count, details
FROM statement_competitor_data_warehouse.bronze_scrape_health
WHERE batch_id = (SELECT batch_id FROM statement_competitor_data_warehouse.bronze_scrape_health ORDER BY checked_at DESC LIMIT 1)
ORDER BY status, shop_name;

-- Health history per shop (spot shops that fail often).
SELECT shop_name, COUNTIF(status = 'FAILED') AS failed, COUNTIF(status = 'WARNING') AS warnings, COUNT(*) AS scrapes
FROM statement_competitor_data_warehouse.bronze_scrape_health
GROUP BY shop_name
ORDER BY failed DESC, warnings DESC;

-- Product count trend for one shop.
SELECT batch_id, checked_at, status, product_count, baseline_count
FROM statement_competitor_data_warehouse.bronze_scrape_health
WHERE shop_name = 'Retro MCM'
ORDER BY checked_at;

-- Override a false FAILED result (the scrape will then count for delisting).
UPDATE statement_competitor_data_warehouse.bronze_scrape_health
SET status = 'HEALTHY', issues = CONCAT('OVERRIDDEN: ', issues)
WHERE batch_id = 'BATCH_YYYYMMDD_HHMMSS' AND shop_name = 'Retro MCM';
*/
