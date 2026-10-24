/*
===============================================================================
Script   : 01_create_dataset.sql
Layer    : All
Purpose  : Creates the dataset that holds every Bronze, Silver and Gold object.
           Layers are separated by table prefix (bronze_, silver_, dim_, fact_,
           ref_, gold_) because BigQuery has no schemas within a dataset.
When     : Initial setup. Run first. Safe to re-run.
Warning  : None. An existing dataset is left unchanged.
===============================================================================
*/

-- Create the dataset if it does not exist.
CREATE SCHEMA IF NOT EXISTS statement_competitor_data_warehouse;


/*
===============================================================================
Tests (run manually)
===============================================================================
-- Dataset exists: fails if the dataset does not exist; returns 0 while it is empty.
SELECT COUNT(*) AS object_count
FROM `statement_competitor_data_warehouse.INFORMATION_SCHEMA.TABLES`;
*/
