/*
=============================================================================
4. GOLD LAYER (Business-Ready Star Schema)
=============================================================================
*/

-- DIMENSION: Shop (Updated IDs to INT64 based on ERD Diagram Page 4)
CREATE OR REPLACE TABLE statement_competitor_data_warehouse.gold_dim_shop (
    shop_id INT64 NOT NULL,
    shop_name STRING,
    region STRING,
    city STRING,
    latitude FLOAT64,
    longitude FLOAT64,
    PRIMARY KEY (shop_id) NOT ENFORCED
);

-- DIMENSION: Material
CREATE OR REPLACE TABLE statement_competitor_data_warehouse.gold_dim_material (
    material_id INT64 NOT NULL,
    material_name STRING,
    PRIMARY KEY (material_id) NOT ENFORCED
);

-- DIMENSION: Maker
CREATE OR REPLACE TABLE statement_competitor_data_warehouse.gold_dim_maker (
    maker_id INT64 NOT NULL,
    maker_name STRING,
    PRIMARY KEY (maker_id) NOT ENFORCED
);

-- DIMENSION: Category
CREATE OR REPLACE TABLE statement_competitor_data_warehouse.gold_dim_category (
    category_id INT64 NOT NULL,
    category_name STRING,
    PRIMARY KEY (category_id) NOT ENFORCED
);

-- FACT (Core Product Hub): fact_product_scrape
CREATE OR REPLACE TABLE statement_competitor_data_warehouse.gold_fact_product_scrape (
    product_id INT64 NOT NULL,
    shop_id INT64,
    category_id INT64,
    maker_id INT64,
    material_id INT64,
    url STRING NOT NULL,
    title STRING,
    description STRING,
    era STRING,
    condition STRING,
    dimension STRING,
    PRIMARY KEY (product_id) NOT ENFORCED,
    FOREIGN KEY (shop_id) REFERENCES statement_competitor_data_warehouse.gold_dim_shop(shop_id) NOT ENFORCED,
    FOREIGN KEY (category_id) REFERENCES statement_competitor_data_warehouse.gold_dim_category(category_id) NOT ENFORCED,
    FOREIGN KEY (maker_id) REFERENCES statement_competitor_data_warehouse.gold_dim_maker(maker_id) NOT ENFORCED,
    FOREIGN KEY (material_id) REFERENCES statement_competitor_data_warehouse.gold_dim_material(material_id) NOT ENFORCED
);

-- DIMENSION / FACT HISTORY: dim_price_history (Type 2 SCD / Snapshot log)
CREATE OR REPLACE TABLE statement_competitor_data_warehouse.gold_dim_price_history (
    price_log_id INT64 NOT NULL,
    product_id INT64,
    scrape_timestamp TIMESTAMP,
    price_zar NUMERIC, -- NUMERIC is best practice for financial data
    available BOOLEAN,
    PRIMARY KEY (price_log_id) NOT ENFORCED,
    FOREIGN KEY (product_id) REFERENCES statement_competitor_data_warehouse.gold_fact_product_scrape(product_id) NOT ENFORCED
);