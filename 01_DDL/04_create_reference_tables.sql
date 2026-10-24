/*
===============================================================================
Script   : 04_create_reference_tables.sql
Layer    : Reference
Purpose  : Creates and seeds the reference tables that hold manually maintained
           shop data:
             ref_shop_locations  verified coordinates (used by the Silver load)
             ref_shop_rules      how each shop signals a sale (used by Gold) and
                                 scrape health thresholds (used by sp_check_scrape_health)
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
    updated_at TIMESTAMP OPTIONS(description="When this rule was last changed"),
    min_count_ratio FLOAT64 OPTIONS(description="Scrape health: FAILED if product count is below this share of the shop's baseline"),
    max_quarantine_rate FLOAT64 OPTIONS(description="Scrape health: FAILED if more than this share of the shop's rows is quarantined"),
    max_duplicate_rate FLOAT64 OPTIONS(description="Scrape health: WARNING if more than this share of the shop's rows repeats a URL"),
    max_field_empty_rate FLOAT64 OPTIONS(description="Scrape health: WARNING if a normally filled field is empty in more than this share of rows"),
    min_expected_ratio FLOAT64 OPTIONS(description="Scrape health: FAILED if products in the CSV are below this share of the count the shop's site reports")
);

-- Add the scrape health columns to an existing table.
ALTER TABLE statement_competitor_data_warehouse.ref_shop_rules
    ADD COLUMN IF NOT EXISTS min_count_ratio FLOAT64 OPTIONS(description="Scrape health: FAILED if product count is below this share of the shop's baseline"),
    ADD COLUMN IF NOT EXISTS max_quarantine_rate FLOAT64 OPTIONS(description="Scrape health: FAILED if more than this share of the shop's rows is quarantined"),
    ADD COLUMN IF NOT EXISTS max_duplicate_rate FLOAT64 OPTIONS(description="Scrape health: WARNING if more than this share of the shop's rows repeats a URL"),
    ADD COLUMN IF NOT EXISTS max_field_empty_rate FLOAT64 OPTIONS(description="Scrape health: WARNING if a normally filled field is empty in more than this share of rows"),
    ADD COLUMN IF NOT EXISTS min_expected_ratio FLOAT64 OPTIONS(description="Scrape health: FAILED if products in the CSV are below this share of the count the shop's site reports");

-- Seed the default rule if it is missing.
MERGE statement_competitor_data_warehouse.ref_shop_rules AS target
USING (SELECT '*' AS shop_name) AS source
ON target.shop_name = source.shop_name
WHEN NOT MATCHED THEN
    INSERT (shop_name, zero_price_means_sold, delist_grace_scrapes, notes, updated_at,
            min_count_ratio, max_quarantine_rate, max_duplicate_rate, max_field_empty_rate, min_expected_ratio)
    VALUES ('*', TRUE, 2, 'Default rule for shops without their own row', CURRENT_TIMESTAMP(),
            0.7, 0.2, 0.05, 0.5, 0.9);

-- Seed known shop exceptions that are not in the table yet.
-- Change them later with sp_set_shop_rule (02_Stored_Procedures/10).
MERGE statement_competitor_data_warehouse.ref_shop_rules AS target
USING (
    SELECT 'Statement Collectables' AS shop_name, 0.3 AS max_quarantine_rate,
           'Removes the price of sold items; up to 30% rows without a price is normal' AS notes
) AS source
ON LOWER(target.shop_name) = LOWER(source.shop_name)
WHEN NOT MATCHED THEN
    INSERT (shop_name, max_quarantine_rate, notes, updated_at)
    VALUES (source.shop_name, source.max_quarantine_rate, source.notes, CURRENT_TIMESTAMP());

-- Fill default scrape health thresholds on the '*' row where still empty.
UPDATE statement_competitor_data_warehouse.ref_shop_rules
SET
    min_count_ratio = COALESCE(min_count_ratio, 0.7),
    max_quarantine_rate = COALESCE(max_quarantine_rate, 0.2),
    max_duplicate_rate = COALESCE(max_duplicate_rate, 0.05),
    max_field_empty_rate = COALESCE(max_field_empty_rate, 0.5),
    min_expected_ratio = COALESCE(min_expected_ratio, 0.9)
WHERE shop_name = '*';


/*
===============================================================================
Tests (run manually)
===============================================================================
-- Shop locations seeded (expect at least 9 rows).
SELECT shop_name, latitude, longitude, updated_at
FROM statement_competitor_data_warehouse.ref_shop_locations
ORDER BY shop_name;

-- Default rule present (expect '*', TRUE, 2, 0.7, 0.2, 0.05, 0.5, 0.9).
SELECT shop_name, zero_price_means_sold, delist_grace_scrapes,
       min_count_ratio, max_quarantine_rate, max_duplicate_rate, max_field_empty_rate, min_expected_ratio, notes
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
-- NULL thresholds fall back to the '*' row.
INSERT INTO statement_competitor_data_warehouse.ref_shop_rules (shop_name, zero_price_means_sold, delist_grace_scrapes, notes, updated_at)
VALUES ('Example Shop', FALSE, 2, 'Lists POA items at R0.00', CURRENT_TIMESTAMP());

-- Relax the product count threshold for a shop with a volatile catalogue.
UPDATE statement_competitor_data_warehouse.ref_shop_rules
SET min_count_ratio = 0.5, updated_at = CURRENT_TIMESTAMP()
WHERE shop_name = 'Example Shop';
*/
