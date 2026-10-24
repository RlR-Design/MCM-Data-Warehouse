/*
===============================================================================
Script   : 09_create_automation_tables.sql
Layer    : Operations
Purpose  : Creates ops_pipeline_trigger_log, one row per bucket upload that the
           pipeline trigger function acted on. Stops duplicate events from
           loading the same file twice.
When     : Initial setup, after 08_create_monitoring_views.sql. Required
           before deploying 05_Automation/pipeline_trigger. Safe to re-run.
Warning  : Do not drop or recreate. To re-run a file that was already
           triggered, CALL sp_run_pipeline manually (see Tests).
===============================================================================
*/

-- -----------------------------------------------------------------------------
-- ops_pipeline_trigger_log: one row per uploaded object version (uri + generation)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS statement_competitor_data_warehouse.ops_pipeline_trigger_log (
    gcs_uri STRING,
    object_generation STRING,
    event_id STRING,
    triggered_at TIMESTAMP
);


/*
===============================================================================
Tests (run manually)
===============================================================================
-- Recent triggers.
SELECT *
FROM statement_competitor_data_warehouse.ops_pipeline_trigger_log
ORDER BY triggered_at DESC
LIMIT 20;

-- Pipeline outcome of recent triggers (completion or error per load).
SELECT error_timestamp, error_type, error_details
FROM statement_competitor_data_warehouse.bronze_raw_error_logs
WHERE scraper_job_id = 'sp_load_bronze_products'
ORDER BY error_timestamp DESC
LIMIT 20;

-- Manual re-run of a file (bypasses the trigger log).
-- CALL statement_competitor_data_warehouse.sp_run_pipeline('gs://<bucket>/<file>.csv');
*/
