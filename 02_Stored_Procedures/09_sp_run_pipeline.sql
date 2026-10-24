/*
===============================================================================
Script   : 09_sp_run_pipeline.sql
Layer    : All
Purpose  : Runs the full pipeline for one scraped CSV, steps 1 to 8 in order.
           Each step raises on failure, which stops the run at that step.
When     : Deploy at setup, after scripts 01 to 08. CALL once per new scrape file.
Requires : All DDL scripts (01_DDL) and procedures 01 to 08 deployed.
Params   : p_gcs_uri  CSV location (required).
Logs     : Each step writes to its own log table.
Warning  : Step 1 appends the whole file to Bronze. Do not run twice for the same file.
===============================================================================
*/

CREATE OR REPLACE PROCEDURE statement_competitor_data_warehouse.sp_run_pipeline (IN p_gcs_uri STRING)
BEGIN
    -- 1. Load the CSV into Bronze as a new batch.
    CALL statement_competitor_data_warehouse.sp_load_bronze_products(p_gcs_uri);

    -- 2. Validate that batch and quarantine failing rows.
    CALL statement_competitor_data_warehouse.sp_validate_bronze_products(NULL);

    -- 3. Check each shop's scrape for failures (flags only, never stops the run).
    CALL statement_competitor_data_warehouse.sp_check_scrape_health(NULL);

    -- 4. Clean the validated batch and merge it into Silver.
    CALL statement_competitor_data_warehouse.sp_load_silver_products(NULL);

    -- 5. Validate the rows written by that load.
    CALL statement_competitor_data_warehouse.sp_validate_silver_products(NULL);

    -- 6. Rebuild the star schema.
    CALL statement_competitor_data_warehouse.sp_normalise_silver_products();

    -- 7. Record price and availability changes.
    CALL statement_competitor_data_warehouse.sp_load_dim_price_history();

    -- 8. Run the integrity checks.
    CALL statement_competitor_data_warehouse.sp_reconciled_silver_products();
END;


/*
===============================================================================
Tests (run manually)
===============================================================================
-- Run the full pipeline for one CSV.
CALL statement_competitor_data_warehouse.sp_run_pipeline('gs://statement_competitor_data_warehouse/MCM_All_products_2026-09-15.csv');

-- Latest event from every step's log (expect INFO_STAGE_COMPLETE for each).
SELECT 'bronze' AS log, scraper_job_id AS step, error_type, error_details, error_timestamp
FROM statement_competitor_data_warehouse.bronze_raw_error_logs
QUALIFY ROW_NUMBER() OVER (PARTITION BY scraper_job_id ORDER BY error_timestamp DESC) = 1
UNION ALL
SELECT 'silver', scraper_job_id, error_type, error_details, error_timestamp
FROM statement_competitor_data_warehouse.silver_transformed_error_logs
QUALIFY ROW_NUMBER() OVER (PARTITION BY scraper_job_id ORDER BY error_timestamp DESC) = 1
UNION ALL
SELECT 'normalised', procedure_name, error_type, error_details, error_timestamp
FROM statement_competitor_data_warehouse.silver_normalised_error_logs
QUALIFY ROW_NUMBER() OVER (PARTITION BY procedure_name ORDER BY error_timestamp DESC) = 1
UNION ALL
SELECT 'historisation', procedure_name, error_type, error_details, error_timestamp
FROM statement_competitor_data_warehouse.silver_historisation_error_logs
QUALIFY ROW_NUMBER() OVER (PARTITION BY procedure_name ORDER BY error_timestamp DESC) = 1
UNION ALL
SELECT 'reconciled', procedure_name, error_type, error_details, error_timestamp
FROM statement_competitor_data_warehouse.silver_reconciled_error_logs
QUALIFY ROW_NUMBER() OVER (PARTITION BY procedure_name ORDER BY error_timestamp DESC) = 1
ORDER BY error_timestamp;
*/
