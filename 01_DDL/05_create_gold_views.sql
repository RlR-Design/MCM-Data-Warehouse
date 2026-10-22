/*
=============================================================================
4. GOLD LAYER (Business-Ready Star Schema)
Dataset: statement_competitor_data_warehouse

Description:
Gold exposes the Silver star schema to business users and reporting tools
(Google Data Studio / Looker Studio). As defined in the Separation of Concerns
diagram, Gold objects are VIEWS with no load step: they always reflect the
current Silver data and cannot drift out of sync.

Column names follow the Entity Relationship Diagram. Pipeline audit columns
(execution_id, batch_id, ingested_at) are left out, as they are for engineers,
not business users.

  gold_dim_shop            -> dim_shop
  gold_dim_category        -> dim_category
  gold_dim_maker           -> dim_maker
  gold_dim_material        -> dim_material
  gold_fact_product_scrape -> fact_product_scrape
  gold_dim_price_history   -> dim_price_history (all versions, with SCD2 columns)

The flat reporting view (gold_products_flat) and the product lifecycle views
are in 08_gold_product_lifecycle.sql.

Re-running this script is safe: views hold no data.
=============================================================================
*/

-- DIMENSION: Shop
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

-- DIMENSION: Category
CREATE OR REPLACE VIEW statement_competitor_data_warehouse.gold_dim_category
OPTIONS(description="Product categories (from dim_category)")
AS
SELECT
    category_id,
    category
FROM statement_competitor_data_warehouse.dim_category;

-- DIMENSION: Maker
CREATE OR REPLACE VIEW statement_competitor_data_warehouse.gold_dim_maker
OPTIONS(description="Makers / designers / brands (from dim_maker). '-1' means unknown.")
AS
SELECT
    maker_id,
    makers
FROM statement_competitor_data_warehouse.dim_maker;

-- DIMENSION: Material
CREATE OR REPLACE VIEW statement_competitor_data_warehouse.gold_dim_material
OPTIONS(description="Primary materials (from dim_material). '-1' means unknown.")
AS
SELECT
    material_id,
    primary_material
FROM statement_competitor_data_warehouse.dim_material;

-- FACT (Core Product Hub): fact_product_scrape
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

-- DIMENSION / FACT HISTORY: dim_price_history (Type 2 SCD)
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
