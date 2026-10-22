/*
=============================================================================
1. CREATE DATASET (DATABASE)
=============================================================================
In BigQuery, a 'SCHEMA' or 'DATABASE' is called a Dataset.
That is why there is one 'statement_data_warehouse' dataset(schema) and prefixes
for the tables with bronze_, silver_, and gold_ to maintain the Medallion Architecture.

Purpose of the script:
    This script checks if the database exists and creates a new database if it does not exist.

Warning:
    Deleting the IF NOT EXISTS statement and running the code will result in the deletion of all the data. Proceed with
    caution.
*/

CREATE SCHEMA IF NOT EXISTS statement_competitor_data_warehouse;
