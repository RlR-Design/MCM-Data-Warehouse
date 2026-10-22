/*
===============================================================================
Script   : 04_create_reference_tables.sql
Layer    : Reference
Purpose  : Creates and seeds the reference tables that hold manually maintained
           shop data:
             ref_shop_locations  verified coordinates (used by the Silver load)
             ref_shop_rules      how each shop signals a sale (used by Gold)
When     : Initial setup, after 03_create_silver_tables.sql. Safe to re-run.
Warning  : Seeds only insert missing rows. Edits made in the tables are kept.
===============================================================================
*/

-- -----------------------------------------------------------------------------
-- ref_shop_locations: one row per shop, matched case-insensitively.
-- Overrides scraped latitude/longitude in sp_load_silver_products.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS statement_competitor_data_warehouse.ref_shop_locations (
    shop_name STRING NOT NULL OPTIONS(description="Shop name exactly as scraped (matched case-insensitively)"),
    latitude FLOAT64 OPTIONS(description="Verified latitude of the shop"),
    longitude FLOAT64 OPTIONS(description="Verified longitude of the shop"),
    updated_at TIMESTAMP OPTIONS(description="When this location was last changed")
);

-- Seed known shops that are not in the table yet.
MERGE statement_competitor_data_warehouse.ref_shop_locations AS target
USING (
    SELECT 'The Thrifty Duo' AS shop_name, -34.09381146 AS latitude, 18.84318164 AS longitude UNION ALL
    SELECT 'The Retro Collection', -26.25018465, 28.11045514 UNION ALL
    SELECT 'Vintageclub', -33.91460233218425, 18.48394626 UNION ALL
    SELECT 'Retro MCM', -25.73971248, 28.2414414 UNION ALL
    SELECT 'Huisraad', -33.89934459, 18.60473736 UNION ALL
    SELECT 'Retrend', -26.14306559, 28.01834357 UNION ALL
    SELECT 'Cosmic Modern', -33.89673833, 18.60654708 UNION ALL
    SELECT 'Statement Collectables', -25.77693766, 28.2585483 UNION ALL
    SELECT 'Amphora_Studio', -26.120134, 27.901464
) AS source
ON LOWER(target.shop_name) = LOWER(source.shop_name)
WHEN NOT MATCHED THEN
    INSERT (shop_name, latitude, longitude, updated_at)
    VALUES (source.shop_name, source.latitude, source.longitude, CURRENT_TIMESTAMP());

-- -----------------------------------------------------------------------------
-- ref_shop_rules: one row per shop, matched case-insensitively.
-- The '*' row is the default for shops without their own row.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS statement_competitor_data_warehouse.ref_shop_rules (
    shop_name STRING NOT NULL OPTIONS(description="Shop name exactly as scraped (case-insensitive), or '*' for the default rule"),
    zero_price_means_sold BOOL OPTIONS(description="TRUE if the shop sets the price to R0.00 when an item has sold"),
    delist_grace_scrapes INT64 OPTIONS(description="Consecutive shop scrapes a product must be missing from before it is treated as delisted"),
    notes STRING OPTIONS(description="Free text describing how this shop behaves"),
    updated_at TIMESTAMP OPTIONS(description="When this rule was last changed")
);

-- Seed the default rule if it is missing.
MERGE statement_competitor_data_warehouse.ref_shop_rules AS target
USING (SELECT '*' AS shop_name) AS source
ON target.shop_name = source.shop_name
WHEN NOT MATCHED THEN
    INSERT (shop_name, zero_price_means_sold, delist_grace_scrapes, notes, updated_at)
    VALUES ('*', TRUE, 2, 'Default rule for shops without their own row', CURRENT_TIMESTAMP());


/*
===============================================================================
Tests (run manually)
===============================================================================
-- Shop locations seeded (expect at least 9 rows).
SELECT shop_name, latitude, longitude, updated_at
FROM statement_competitor_data_warehouse.ref_shop_locations
ORDER BY shop_name;

-- Default rule present (expect '*', TRUE, 2).
SELECT shop_name, zero_price_means_sold, delist_grace_scrapes, notes
FROM statement_competitor_data_warehouse.ref_shop_rules
ORDER BY shop_name;

-- No duplicate shop names (expect no rows).
SELECT 'ref_shop_locations' AS table_name, LOWER(shop_name) AS shop, COUNT(*) AS n
FROM statement_competitor_data_warehouse.ref_shop_locations GROUP BY 2 HAVING COUNT(*) > 1
UNION ALL
SELECT 'ref_shop_rules', LOWER(shop_name), COUNT(*)
FROM statement_competitor_data_warehouse.ref_shop_rules GROUP BY 2 HAVING COUNT(*) > 1;

-- Add or correct a shop location.
INSERT INTO statement_competitor_data_warehouse.ref_shop_locations (shop_name, latitude, longitude, updated_at)
VALUES ('New Shop', -33.9249, 18.4241, CURRENT_TIMESTAMP());

-- Add a shop-specific rule (e.g. R0.00 means "price on request", not sold).
INSERT INTO statement_competitor_data_warehouse.ref_shop_rules (shop_name, zero_price_means_sold, delist_grace_scrapes, notes, updated_at)
VALUES ('Example Shop', FALSE, 2, 'Lists POA items at R0.00', CURRENT_TIMESTAMP());
*/
