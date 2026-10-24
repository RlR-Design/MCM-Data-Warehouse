/*
===============================================================================
Script   : 00_reset_dataset.sql
Layer    : All
Purpose  : Drops every table, view and procedure in the dataset for a clean
           rebuild. The dataset itself is kept, so its location and access
           settings stay unchanged.
When     : One-off, before the first full build. Then run 01_DDL/01 to 09 and
           deploy 02_Stored_Procedures/01 to 10.
Requires : Nothing.
Warning  : DESTROYS ALL DATA, INCLUDING BRONZE AND PRICE HISTORY.
           Never run once real history exists.
           The script is commented out. To run it, uncomment the block, set
           v_confirm to 'DROP EVERYTHING' and run the whole block at once.
===============================================================================
*/

/*
-- Single block so DataGrip sends the script as one statement.
BEGIN
  DECLARE v_confirm STRING DEFAULT 'NO';

  -- Guard: stop unless the confirmation text is set.
  IF v_confirm != 'DROP EVERYTHING' THEN
    RAISE USING MESSAGE = "Reset not confirmed. Set v_confirm to 'DROP EVERYTHING'.";
  END IF;

  -- Drop views first, then tables.
  FOR obj IN (
    SELECT table_name, table_type
    FROM `statement_competitor_data_warehouse.INFORMATION_SCHEMA.TABLES`
    ORDER BY IF(table_type = 'VIEW', 0, 1), table_name
  ) DO
    EXECUTE IMMEDIATE FORMAT(
      'DROP %s IF EXISTS `statement_competitor_data_warehouse.%s`',
      CASE obj.table_type
        WHEN 'VIEW'              THEN 'VIEW'
        WHEN 'MATERIALIZED VIEW' THEN 'MATERIALIZED VIEW'
        WHEN 'EXTERNAL'          THEN 'EXTERNAL TABLE'
        WHEN 'SNAPSHOT'          THEN 'SNAPSHOT TABLE'
        ELSE 'TABLE'
      END,
      obj.table_name);
  END FOR;

  -- Drop procedures and functions.
  FOR r IN (
    SELECT routine_name, routine_type
    FROM `statement_competitor_data_warehouse.INFORMATION_SCHEMA.ROUTINES`
  ) DO
    EXECUTE IMMEDIATE FORMAT(
      'DROP %s IF EXISTS `statement_competitor_data_warehouse.%s`',
      CASE r.routine_type
        WHEN 'PROCEDURE'      THEN 'PROCEDURE'
        WHEN 'TABLE FUNCTION' THEN 'TABLE FUNCTION'
        ELSE 'FUNCTION'
      END,
      r.routine_name);
  END FOR;
END;
*/


/*
===============================================================================
Tests (run manually)
===============================================================================
-- Before the reset: what will be dropped.
SELECT table_type, COUNT(*) AS objects
FROM `statement_competitor_data_warehouse.INFORMATION_SCHEMA.TABLES`
GROUP BY table_type;

SELECT routine_type, COUNT(*) AS objects
FROM `statement_competitor_data_warehouse.INFORMATION_SCHEMA.ROUTINES`
GROUP BY routine_type;

-- After the reset: both queries return no rows.
*/
