/*
===============================================================================
Script   : 05_create_gold_views.sql
Layer    : Gold
Purpose  : Exposes the Silver star schema to business users and reporting tools
           as views. Column names follow the ERD. Pipeline audit columns
           (execution_id, batch_id, ingested_at) are excluded.
When     : Initial setup, after 04_create_reference_tables.sql.
           Re-run after any change to this file. Safe to re-run.
Warning  : Views hold no data. If an old Gold TABLE with the same name exists,
           drop it first: CREATE OR REPLACE VIEW cannot replace a table.
===============================================================================
*/

-- -----------------------------------------------------------------------------
-- gold_dim_shop: competitor shops with location
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW statement_competitor_data_warehouse.gold_dim_shop
OPTIONS(description="Competitor shops with location (from dim_shop)")
AS
SELECT
    shop_id,
    shop_name,
    region,
    city,
    latitude,
    longitude
FROM statement_competitor_data_warehouse.dim_shop;

-- -----------------------------------------------------------------------------
-- gold_dim_category: product categories
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW statement_competitor_data_warehouse.gold_dim_category
OPTIONS(description="Product categories (from dim_category)")
AS
SELECT
    category_id,
    category
FROM statement_competitor_data_warehouse.dim_category;

-- -----------------------------------------------------------------------------
-- gold_dim_maker: makers, designers and brands ('-1' = unknown)
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW statement_competitor_data_warehouse.gold_dim_maker
OPTIONS(description="Makers / designers / brands (from dim_maker). '-1' means unknown.")
AS
SELECT
    maker_id,
    makers
FROM statement_competitor_data_warehouse.dim_maker;

-- -----------------------------------------------------------------------------
-- gold_dim_material: primary materials ('-1' = unknown)
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW statement_competitor_data_warehouse.gold_dim_material
OPTIONS(description="Primary materials (from dim_material). '-1' means unknown.")
AS
SELECT
    material_id,
    primary_material
FROM statement_competitor_data_warehouse.dim_material;

-- -----------------------------------------------------------------------------
-- gold_fact_product_scrape: one row per product with dimension keys
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW statement_competitor_data_warehouse.gold_fact_product_scrape
OPTIONS(description="One row per competitor product with keys to the shop, category, maker and material dimensions")
AS
SELECT
    product_id,
    shop_id,
    category_id,
    maker_id,
    material_id,
    url,
    title,
    description,
    era,
    condition,
    dimensions
FROM statement_competitor_data_warehouse.fact_product_scrape;

-- -----------------------------------------------------------------------------
-- gold_dim_price_history: every price/availability version (SCD Type 2)
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW statement_competitor_data_warehouse.gold_dim_price_history
OPTIONS(description="Every price/availability version per product (SCD Type 2). is_current = TRUE is the latest version.")
AS
SELECT
    price_log_id,
    product_id,
    scrape_timestamp,
    price,
    available,
    valid_from,
    valid_to,
    is_current
FROM statement_competitor_data_warehouse.dim_price_history;


/*
===============================================================================
Tests (run manually)
===============================================================================
-- All six Gold star schema views exist (expect table_type = VIEW).
SELECT table_name, table_type
FROM `statement_competitor_data_warehouse.INFORMATION_SCHEMA.TABLES`
WHERE table_name IN ('gold_dim_shop', 'gold_dim_category', 'gold_dim_maker',
                     'gold_dim_material', 'gold_fact_product_scrape', 'gold_dim_price_history')
ORDER BY table_name;

-- Views return the same row counts as their Silver sources.
SELECT 'gold_dim_shop' AS view_name,
       (SELECT COUNT(*) FROM statement_competitor_data_warehouse.gold_dim_shop) AS view_rows,
       (SELECT COUNT(*) FROM statement_competitor_data_warehouse.dim_shop) AS source_rows
UNION ALL
SELECT 'gold_fact_product_scrape',
       (SELECT COUNT(*) FROM statement_competitor_data_warehouse.gold_fact_product_scrape),
       (SELECT COUNT(*) FROM statement_competitor_data_warehouse.fact_product_scrape)
UNION ALL
SELECT 'gold_dim_price_history',
       (SELECT COUNT(*) FROM statement_competitor_data_warehouse.gold_dim_price_history),
       (SELECT COUNT(*) FROM statement_competitor_data_warehouse.dim_price_history);

-- One-off migration: drop the old, never-loaded Gold tables (check row_count = 0 first).
SELECT table_id, row_count
FROM statement_competitor_data_warehouse.__TABLES__
WHERE table_id LIKE 'gold_%';

DROP TABLE IF EXISTS statement_competitor_data_warehouse.gold_dim_price_history;
DROP TABLE IF EXISTS statement_competitor_data_warehouse.gold_fact_product_scrape;
DROP TABLE IF EXISTS statement_competitor_data_warehouse.gold_dim_shop;
DROP TABLE IF EXISTS statement_competitor_data_warehouse.gold_dim_category;
DROP TABLE IF EXISTS statement_competitor_data_warehouse.gold_dim_maker;
DROP TABLE IF EXISTS statement_competitor_data_warehouse.gold_dim_material;
*/
