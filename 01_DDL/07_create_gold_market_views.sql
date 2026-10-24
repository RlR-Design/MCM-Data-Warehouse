/*
===============================================================================
Script   : 07_create_gold_market_views.sql
Layer    : Gold
Purpose  : Market insight views for the client's business objectives (README):
             gold_market_products  base: one row per product, business labels
             gold_sales            every sale nationally, one row per sale
             gold_price_baseline   price quartiles per segment (pricing baseline)
             gold_value_drivers    price premiums by material, maker, era, condition
             gold_market_monthly   new listings and sales per month, region, category
             gold_regional_demand  demand and velocity per region (all provinces)
             gold_shop_scorecard   competitor benchmark per shop
           Prices: asking_price = last non-zero listed price; sold_price = last
           real price before the sale. Medians are approximate (APPROX_QUANTILES).
When     : Initial setup, after 06_create_gold_lifecycle_views.sql.
           Re-run after any change to this file. Safe to re-run.
Warning  : Views hold no data. Medians cannot be re-aggregated: filter, do not
           sum or average median columns in Data Studio.
===============================================================================
*/

-- -----------------------------------------------------------------------------
-- gold_market_products: one row per product with business-friendly labels.
-- '-1' placeholders become 'Unknown'. Base for every view below.
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW statement_competitor_data_warehouse.gold_market_products
OPTIONS(description="One row per product with business labels, status flags and prices; base for the Gold market views")
AS
SELECT
    product_id,
    url,
    title,
    shop_name,
    COALESCE(NULLIF(region, ''), 'Unknown') AS region,
    COALESCE(NULLIF(city, ''), 'Unknown') AS city,
    COALESCE(category, 'Uncategorised') AS category,
    IF(makers IS NULL OR makers = '-1', 'Unknown', makers) AS maker,
    IF(primary_material IS NULL OR primary_material = '-1', 'Unknown', primary_material) AS material,
    IF(era IS NULL OR era = '-1', 'Unknown', era) AS era,
    IF(condition IS NULL OR condition = '-1', 'Unknown', condition) AS condition,
    status,
    status = 'SOLD' AS is_sold,
    status IN ('ACTIVE', 'ACTIVE_DISCOUNTED', 'PENDING_DELIST') AS is_active,
    sold_reason,
    sold_at,
    DATE_TRUNC(DATE(sold_at), MONTH) AS sale_month,
    sold_price,
    last_listed_price AS asking_price,
    first_listed_price,
    current_price,
    markdown_count,
    total_markdown_pct,
    days_on_market,
    first_seen_at,
    DATE_TRUNC(DATE(first_seen_at), MONTH) AS listing_month,
    last_seen_at
FROM statement_competitor_data_warehouse.gold_products_flat
-- Products without price history have no status yet.
WHERE status IS NOT NULL;

-- -----------------------------------------------------------------------------
-- gold_sales: every sale in the country, one row per sold product
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW statement_competitor_data_warehouse.gold_sales
OPTIONS(description="Every sale nationally: one row per sold product with price, discount and days on market")
AS
SELECT
    product_id,
    url,
    title,
    shop_name,
    region,
    city,
    category,
    maker,
    material,
    era,
    condition,
    sold_at,
    sale_month,
    sold_price,
    first_listed_price,
    total_markdown_pct,
    markdown_count,
    days_on_market,
    sold_reason
FROM statement_competitor_data_warehouse.gold_market_products
WHERE is_sold;

-- -----------------------------------------------------------------------------
-- gold_price_baseline: price quartiles per segment (long format).
-- segment_type: All products, Category, Material, Maker, Era, Condition, Region, Shop
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW statement_competitor_data_warehouse.gold_price_baseline
OPTIONS(description="Asking and sold price quartiles, mean and sell-through per segment (category, material, maker, era, condition, region, shop)")
AS
-- One row per product per segment it belongs to.
WITH segmented AS (
    SELECT
        s.segment_type,
        s.segment,
        p.is_active,
        p.is_sold,
        CAST(p.asking_price AS FLOAT64) AS asking_price,
        CAST(p.sold_price AS FLOAT64) AS sold_price,
        p.days_on_market
    FROM statement_competitor_data_warehouse.gold_market_products p,
    UNNEST([
        STRUCT('All products' AS segment_type, 'All' AS segment),
        STRUCT('Category' AS segment_type, p.category AS segment),
        STRUCT('Material' AS segment_type, p.material AS segment),
        STRUCT('Maker' AS segment_type, p.maker AS segment),
        STRUCT('Era' AS segment_type, p.era AS segment),
        STRUCT('Condition' AS segment_type, p.condition AS segment),
        STRUCT('Region' AS segment_type, p.region AS segment),
        STRUCT('Shop' AS segment_type, p.shop_name AS segment)
    ]) AS s
),
-- Quartiles per segment: [min, p25, median, p75, max].
stats AS (
    SELECT
        segment_type,
        segment,
        COUNT(*) AS products,
        COUNTIF(is_active) AS active_products,
        COUNTIF(is_sold) AS sold_products,
        APPROX_QUANTILES(asking_price, 4) AS asking_q,
        AVG(asking_price) AS asking_mean,
        APPROX_QUANTILES(sold_price, 4) AS sold_q,
        AVG(sold_price) AS sold_mean,
        APPROX_QUANTILES(IF(is_sold, days_on_market, NULL), 2) AS days_q
    FROM segmented
    GROUP BY segment_type, segment
)
SELECT
    segment_type,
    segment,
    products,
    active_products,
    sold_products,
    ROUND(SAFE_DIVIDE(sold_products, products) * 100, 1) AS sell_through_pct,
    ROUND(asking_q[SAFE_OFFSET(0)], 2) AS asking_min,
    ROUND(asking_q[SAFE_OFFSET(1)], 2) AS asking_p25,
    ROUND(asking_q[SAFE_OFFSET(2)], 2) AS asking_median,
    ROUND(asking_q[SAFE_OFFSET(3)], 2) AS asking_p75,
    ROUND(asking_q[SAFE_OFFSET(4)], 2) AS asking_max,
    ROUND(asking_mean, 2) AS asking_mean,
    ROUND(sold_q[SAFE_OFFSET(1)], 2) AS sold_p25,
    ROUND(sold_q[SAFE_OFFSET(2)], 2) AS sold_median,
    ROUND(sold_q[SAFE_OFFSET(3)], 2) AS sold_p75,
    ROUND(sold_mean, 2) AS sold_mean,
    days_q[SAFE_OFFSET(1)] AS median_days_on_market
FROM stats;

-- -----------------------------------------------------------------------------
-- gold_value_drivers: price premium of each material, maker, era and condition
-- against the median of its category ('All categories' compares market-wide).
-- premium_pct = attribute median / category median - 1. is_reliable = 5+ products.
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW statement_competitor_data_warehouse.gold_value_drivers
OPTIONS(description="Median price premium (%) of materials, makers, eras and conditions within each category and market-wide")
AS
-- One row per product per category scope and attribute.
WITH attributes AS (
    SELECT
        scope AS category,
        a.attribute_type,
        a.attribute,
        p.is_sold,
        CAST(p.asking_price AS FLOAT64) AS asking_price,
        CAST(p.sold_price AS FLOAT64) AS sold_price
    FROM statement_competitor_data_warehouse.gold_market_products p,
    UNNEST([p.category, 'All categories']) AS scope,
    UNNEST([
        STRUCT('Material' AS attribute_type, p.material AS attribute),
        STRUCT('Maker' AS attribute_type, p.maker AS attribute),
        STRUCT('Era' AS attribute_type, p.era AS attribute),
        STRUCT('Condition' AS attribute_type, p.condition AS attribute)
    ]) AS a
    WHERE p.asking_price IS NOT NULL
),
-- Median asking price per category scope.
category_baseline AS (
    SELECT
        scope AS category,
        COUNT(*) AS category_products,
        APPROX_QUANTILES(CAST(p.asking_price AS FLOAT64), 2)[SAFE_OFFSET(1)] AS category_median_asking
    FROM statement_competitor_data_warehouse.gold_market_products p,
    UNNEST([p.category, 'All categories']) AS scope
    WHERE p.asking_price IS NOT NULL
    GROUP BY scope
),
-- Median prices per attribute within each category scope.
attribute_stats AS (
    SELECT
        category,
        attribute_type,
        attribute,
        COUNT(*) AS products,
        COUNTIF(is_sold) AS sold_products,
        APPROX_QUANTILES(asking_price, 2)[SAFE_OFFSET(1)] AS median_asking,
        APPROX_QUANTILES(sold_price, 2)[SAFE_OFFSET(1)] AS median_sold
    FROM attributes
    GROUP BY category, attribute_type, attribute
)
SELECT
    a.category,
    a.attribute_type,
    a.attribute,
    a.products,
    a.sold_products,
    ROUND(a.median_asking, 2) AS median_asking_price,
    ROUND(a.median_sold, 2) AS median_sold_price,
    c.category_products,
    ROUND(c.category_median_asking, 2) AS category_median_asking_price,
    ROUND((SAFE_DIVIDE(a.median_asking, c.category_median_asking) - 1) * 100, 1) AS premium_pct,
    a.products >= 5 AS is_reliable
FROM attribute_stats a
JOIN category_baseline c ON c.category = a.category;

-- -----------------------------------------------------------------------------
-- gold_market_monthly: new listings and sales per month, region and category.
-- Counts and values can be summed; medians apply to the row only.
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW statement_competitor_data_warehouse.gold_market_monthly
OPTIONS(description="Monthly new listings, sales, sold value and median prices per region and category")
AS
-- New listings per month (by first sighting).
WITH listings AS (
    SELECT
        listing_month AS month,
        region,
        category,
        COUNT(*) AS new_listings,
        APPROX_QUANTILES(CAST(first_listed_price AS FLOAT64), 2)[SAFE_OFFSET(1)] AS median_new_listing_price
    FROM statement_competitor_data_warehouse.gold_market_products
    GROUP BY month, region, category
),
-- Sales per month (by sold date). Not named 'sales': that would clash with the column.
monthly_sales AS (
    SELECT
        sale_month AS month,
        region,
        category,
        COUNT(*) AS sales,
        SUM(sold_price) AS sold_value,
        APPROX_QUANTILES(CAST(sold_price AS FLOAT64), 2)[SAFE_OFFSET(1)] AS median_sold_price,
        APPROX_QUANTILES(days_on_market, 2)[SAFE_OFFSET(1)] AS median_days_on_market,
        AVG(total_markdown_pct) AS avg_markdown_pct
    FROM statement_competitor_data_warehouse.gold_market_products
    WHERE is_sold
    GROUP BY month, region, category
)
SELECT
    month,
    region,
    category,
    COALESCE(new_listings, 0) AS new_listings,
    ROUND(median_new_listing_price, 2) AS median_new_listing_price,
    COALESCE(sales, 0) AS sales,
    COALESCE(sold_value, 0) AS sold_value,
    ROUND(median_sold_price, 2) AS median_sold_price,
    median_days_on_market,
    ROUND(avg_markdown_pct, 1) AS avg_markdown_pct_before_sale
FROM listings
FULL OUTER JOIN monthly_sales USING (month, region, category);

-- -----------------------------------------------------------------------------
-- gold_regional_demand: demand, stock and velocity per region (all provinces)
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW statement_competitor_data_warehouse.gold_regional_demand
OPTIONS(description="Per region: shops, active stock, sales, sold value, median prices, days on market, sell-through and share of national sales")
AS
SELECT
    region,
    COUNT(DISTINCT shop_name) AS shops,
    COUNT(*) AS products_tracked,
    COUNTIF(is_active) AS active_listings,
    ROUND(SUM(IF(is_active, current_price, 0)), 2) AS active_stock_value,
    ROUND(APPROX_QUANTILES(IF(is_active, CAST(asking_price AS FLOAT64), NULL), 2)[SAFE_OFFSET(1)], 2) AS median_active_asking_price,
    COUNTIF(is_sold) AS sales,
    COUNTIF(is_sold AND sold_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 90 DAY)) AS sales_last_90_days,
    ROUND(SUM(IF(is_sold, sold_price, 0)), 2) AS sold_value,
    ROUND(APPROX_QUANTILES(IF(is_sold, CAST(sold_price AS FLOAT64), NULL), 2)[SAFE_OFFSET(1)], 2) AS median_sold_price,
    APPROX_QUANTILES(IF(is_sold, days_on_market, NULL), 2)[SAFE_OFFSET(1)] AS median_days_on_market,
    ROUND(SAFE_DIVIDE(COUNTIF(is_sold), COUNT(*)) * 100, 1) AS sell_through_pct,
    ROUND(SAFE_DIVIDE(COUNTIF(is_sold), SUM(COUNTIF(is_sold)) OVER ()) * 100, 1) AS share_of_national_sales_pct
FROM statement_competitor_data_warehouse.gold_market_products
GROUP BY region;

-- -----------------------------------------------------------------------------
-- gold_shop_scorecard: competitor benchmark, one row per shop
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW statement_competitor_data_warehouse.gold_shop_scorecard
OPTIONS(description="Per shop: active stock, discounting, sales, sold value, median prices, days on market, sell-through and market share")
AS
SELECT
    shop_name,
    ANY_VALUE(region) AS region,
    ANY_VALUE(city) AS city,
    COUNT(*) AS products_tracked,
    COUNTIF(is_active) AS active_listings,
    COUNTIF(status = 'ACTIVE_DISCOUNTED') AS discounted_listings,
    COUNTIF(status = 'PENDING_DELIST') AS pending_delist,
    ROUND(SUM(IF(is_active, current_price, 0)), 2) AS active_stock_value,
    ROUND(APPROX_QUANTILES(IF(is_active, CAST(asking_price AS FLOAT64), NULL), 2)[SAFE_OFFSET(1)], 2) AS median_active_asking_price,
    COUNTIF(is_sold) AS sales,
    COUNTIF(is_sold AND sold_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)) AS sales_last_30_days,
    COUNTIF(is_sold AND sold_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 90 DAY)) AS sales_last_90_days,
    ROUND(SUM(IF(is_sold, sold_price, 0)), 2) AS sold_value,
    ROUND(APPROX_QUANTILES(IF(is_sold, CAST(sold_price AS FLOAT64), NULL), 2)[SAFE_OFFSET(1)], 2) AS median_sold_price,
    APPROX_QUANTILES(IF(is_sold, days_on_market, NULL), 2)[SAFE_OFFSET(1)] AS median_days_on_market,
    ROUND(AVG(IF(is_sold, total_markdown_pct, NULL)), 1) AS avg_markdown_pct_before_sale,
    ROUND(SAFE_DIVIDE(COUNTIF(is_sold), COUNT(*)) * 100, 1) AS sell_through_pct,
    ROUND(SAFE_DIVIDE(COUNTIF(is_sold), SUM(COUNTIF(is_sold)) OVER ()) * 100, 1) AS share_of_market_sales_pct
FROM statement_competitor_data_warehouse.gold_market_products
GROUP BY shop_name;


/*
===============================================================================
Tests (run manually)
===============================================================================
-- All seven market views exist (expect table_type = VIEW).
SELECT table_name, table_type
FROM `statement_competitor_data_warehouse.INFORMATION_SCHEMA.TABLES`
WHERE table_name IN ('gold_market_products', 'gold_sales', 'gold_price_baseline', 'gold_value_drivers',
                     'gold_market_monthly', 'gold_regional_demand', 'gold_shop_scorecard')
ORDER BY table_name;

-- Base view covers every validated product (expect equal counts; QUARANTINED products are excluded).
SELECT
    (SELECT COUNT(*) FROM statement_competitor_data_warehouse.gold_market_products) AS market_products,
    (SELECT COUNTIF(status != 'QUARANTINED') FROM statement_competitor_data_warehouse.gold_product_status) AS status_products;

-- Sales reconcile across views (expect three equal counts).
SELECT
    (SELECT COUNT(*) FROM statement_competitor_data_warehouse.gold_sales) AS sales_rows,
    (SELECT SUM(sales) FROM statement_competitor_data_warehouse.gold_regional_demand) AS regional_sales,
    (SELECT SUM(sales) FROM statement_competitor_data_warehouse.gold_market_monthly) AS monthly_sales;

-- National pricing baseline and per-category baselines.
SELECT segment_type, segment, products, asking_p25, asking_median, asking_p75, sold_median, sell_through_pct
FROM statement_competitor_data_warehouse.gold_price_baseline
WHERE segment_type IN ('All products', 'Category')
ORDER BY segment_type, products DESC;

-- Strongest reliable material premiums market-wide.
SELECT attribute, products, median_asking_price, category_median_asking_price, premium_pct
FROM statement_competitor_data_warehouse.gold_value_drivers
WHERE category = 'All categories' AND attribute_type = 'Material' AND is_reliable
ORDER BY premium_pct DESC;

-- Regional demand (share of national sales sums to 100).
SELECT region, shops, active_listings, sales, median_sold_price, median_days_on_market, share_of_national_sales_pct
FROM statement_competitor_data_warehouse.gold_regional_demand
ORDER BY sales DESC;

-- Competitor benchmark.
SELECT shop_name, region, active_listings, sales_last_90_days, median_sold_price, median_days_on_market,
       avg_markdown_pct_before_sale, sell_through_pct, share_of_market_sales_pct
FROM statement_competitor_data_warehouse.gold_shop_scorecard
ORDER BY sales DESC;
*/
