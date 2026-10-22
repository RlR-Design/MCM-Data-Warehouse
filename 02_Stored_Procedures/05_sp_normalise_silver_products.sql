-- STREAMING_CHUNK: Setting up DDL for Star Schema dimension tables...
/*
=============================================================================

Dataset: statement_competitor_data_warehouse
Procedure: sp_normalise_silver_products

Description:
This script builds the dimension tables and the central normalised fact table
(`fact_product_scrape`) according to the Star Schema ER Diagram.
It also sets up the required error logging table for the normalisation process.
=============================================================================
*/
-- STREAMING_CHUNK: Declaring the normalisation stored procedure and variables...
/*
=============================================================================
STORED PROCEDURE: sp_normalise_silver_products
Description: Maps primary and foreign keys, extracts unique dimension records,
and merges fact data into `fact_product_scrape`.
=============================================================================
*/
CREATE OR REPLACE PROCEDURE statement_competitor_data_warehouse.sp_normalise_silver_products()
OPTIONS(strict_mode=false)
BEGIN
    DECLARE start_time TIMESTAMP;
    DECLARE v_execution_id STRING;
    DECLARE v_batch_id STRING;
    DECLARE v_error_msg STRING;
    DECLARE v_error_type STRING;

    SET start_time = CURRENT_TIMESTAMP();
    SET v_execution_id = GENERATE_UUID();
    SET v_batch_id = FORMAT_TIMESTAMP('BATCH_NORM_%Y%m%d_%H%M%S', start_time);

    -- STREAMING_CHUNK: Logging start of normalisation process...
    INSERT INTO statement_competitor_data_warehouse.silver_normalised_error_logs (
        log_id, execution_id, batch_id, error_timestamp, procedure_name, error_type, error_details
    )
    VALUES (
        GENERATE_UUID(), v_execution_id, v_batch_id, CURRENT_TIMESTAMP(),
        'sp_normalise_silver_products', 'INFO_STAGE_START',
        'Starting Star Schema Normalisation: Extracting dimensions and generating INT64 Surrogate Keys via FARM_FINGERPRINT.'
    );

    BEGIN
        -- STREAMING_CHUNK: Normalising dimension keys...
        -- Dimension keys are built from a normalised form of each value (lower case,
        -- trimmed, single spaces), so "Teak", "teak" and "Teak " share one key.
        -- The display name stored in the dimension is the most common original spelling.
        CREATE TEMP TABLE stg_normalised AS
        SELECT
            url,
            title,
            description,
            era,
            condition,
            dimensions,
            timestamp_date,
            timestamp_time,
            region,
            city,
            latitude,
            longitude,
            COALESCE(shop_name, 'Unknown') AS shop_name,
            COALESCE(category, 'Uncategorised') AS category,
            COALESCE(makers, '-1') AS makers,
            COALESCE(primary_material, '-1') AS primary_material,
            LOWER(REGEXP_REPLACE(TRIM(COALESCE(shop_name, 'Unknown')), r'\s+', ' ')) AS shop_key,
            LOWER(REGEXP_REPLACE(TRIM(COALESCE(category, 'Uncategorised')), r'\s+', ' ')) AS category_key,
            LOWER(REGEXP_REPLACE(TRIM(COALESCE(makers, '-1')), r'\s+', ' ')) AS maker_key,
            LOWER(REGEXP_REPLACE(TRIM(COALESCE(primary_material, '-1')), r'\s+', ' ')) AS material_key
        FROM statement_competitor_data_warehouse.silver_transformed_products;

        -- STREAMING_CHUNK: Populating dimension tables (Shop & Category)...
        -- MERGE dim_shop: most common known region/city/location per shop.
        -- Existing shops are updated when their attributes change.
        MERGE statement_competitor_data_warehouse.dim_shop AS target
        USING (
            SELECT
                shop_id,
                shop_name,
                region,
                city,
                location.latitude AS latitude,
                location.longitude AS longitude
            FROM (
                SELECT
                    FARM_FINGERPRINT(shop_key) AS shop_id,
                    (SELECT v FROM UNNEST(ARRAY_AGG(shop_name)) AS v GROUP BY v ORDER BY COUNT(*) DESC, v LIMIT 1) AS shop_name,
                    COALESCE(
                        (SELECT v FROM UNNEST(ARRAY_AGG(NULLIF(region, 'Unknown') IGNORE NULLS)) AS v GROUP BY v ORDER BY COUNT(*) DESC, v LIMIT 1),
                        'Unknown'
                    ) AS region,
                    COALESCE(
                        (SELECT v FROM UNNEST(ARRAY_AGG(NULLIF(city, 'Unknown') IGNORE NULLS)) AS v GROUP BY v ORDER BY COUNT(*) DESC, v LIMIT 1),
                        'Unknown'
                    ) AS city,
                    (
                        SELECT AS STRUCT v.latitude, v.longitude
                        FROM UNNEST(ARRAY_AGG(
                            IF(latitude IS NOT NULL AND longitude IS NOT NULL, STRUCT(latitude, longitude), NULL)
                            IGNORE NULLS
                        )) AS v
                        GROUP BY v.latitude, v.longitude
                        ORDER BY COUNT(*) DESC
                        LIMIT 1
                    ) AS location
                FROM stg_normalised
                GROUP BY shop_key
            )
        ) AS source
        ON target.shop_id = source.shop_id
        WHEN MATCHED AND (
               target.shop_name IS DISTINCT FROM source.shop_name
            OR target.region IS DISTINCT FROM source.region
            OR target.city IS DISTINCT FROM source.city
            OR target.latitude IS DISTINCT FROM source.latitude
            OR target.longitude IS DISTINCT FROM source.longitude
        ) THEN
            UPDATE SET
                shop_name = source.shop_name,
                region = source.region,
                city = source.city,
                latitude = source.latitude,
                longitude = source.longitude
        WHEN NOT MATCHED THEN
            INSERT (shop_id, shop_name, region, city, latitude, longitude)
            VALUES (source.shop_id, source.shop_name, source.region, source.city, source.latitude, source.longitude);

        -- MERGE dim_category
        MERGE statement_competitor_data_warehouse.dim_category AS target
        USING (
            SELECT
                FARM_FINGERPRINT(category_key) AS category_id,
                (SELECT v FROM UNNEST(ARRAY_AGG(category)) AS v GROUP BY v ORDER BY COUNT(*) DESC, v LIMIT 1) AS category
            FROM stg_normalised
            GROUP BY category_key
        ) AS source
        ON target.category_id = source.category_id
        WHEN MATCHED AND target.category IS DISTINCT FROM source.category THEN
            UPDATE SET category = source.category
        WHEN NOT MATCHED THEN
            INSERT (category_id, category)
            VALUES (source.category_id, source.category);

        -- STREAMING_CHUNK: Populating dimension tables (Maker & Material)...
        -- MERGE dim_maker
        MERGE statement_competitor_data_warehouse.dim_maker AS target
        USING (
            SELECT
                FARM_FINGERPRINT(maker_key) AS maker_id,
                (SELECT v FROM UNNEST(ARRAY_AGG(makers)) AS v GROUP BY v ORDER BY COUNT(*) DESC, v LIMIT 1) AS makers
            FROM stg_normalised
            GROUP BY maker_key
        ) AS source
        ON target.maker_id = source.maker_id
        WHEN MATCHED AND target.makers IS DISTINCT FROM source.makers THEN
            UPDATE SET makers = source.makers
        WHEN NOT MATCHED THEN
            INSERT (maker_id, makers)
            VALUES (source.maker_id, source.makers);

        -- MERGE dim_material
        MERGE statement_competitor_data_warehouse.dim_material AS target
        USING (
            SELECT
                FARM_FINGERPRINT(material_key) AS material_id,
                (SELECT v FROM UNNEST(ARRAY_AGG(primary_material)) AS v GROUP BY v ORDER BY COUNT(*) DESC, v LIMIT 1) AS primary_material
            FROM stg_normalised
            GROUP BY material_key
        ) AS source
        ON target.material_id = source.material_id
        WHEN MATCHED AND target.primary_material IS DISTINCT FROM source.primary_material THEN
            UPDATE SET primary_material = source.primary_material
        WHEN NOT MATCHED THEN
            INSERT (material_id, primary_material)
            VALUES (source.material_id, source.primary_material);

        -- STREAMING_CHUNK: Upserting normalised data into the target fact table...
        -- MERGE fact_product_scrape (Fact Table mapping). Only changed rows are rewritten.
        MERGE statement_competitor_data_warehouse.fact_product_scrape AS target
        USING (
            SELECT
                FARM_FINGERPRINT(url) AS product_id,
                FARM_FINGERPRINT(shop_key) AS shop_id,
                FARM_FINGERPRINT(category_key) AS category_id,
                FARM_FINGERPRINT(maker_key) AS maker_id,
                FARM_FINGERPRINT(material_key) AS material_id,
                url,
                title,
                description,
                era,
                condition,
                dimensions
            FROM stg_normalised
            -- Pick the most recent record per URL to handle potential duplicates reliably
            QUALIFY ROW_NUMBER() OVER (PARTITION BY url ORDER BY timestamp_date DESC, timestamp_time DESC) = 1
        ) AS source
        ON target.url = source.url
        WHEN MATCHED AND (
               target.shop_id IS DISTINCT FROM source.shop_id
            OR target.category_id IS DISTINCT FROM source.category_id
            OR target.maker_id IS DISTINCT FROM source.maker_id
            OR target.material_id IS DISTINCT FROM source.material_id
            OR target.title IS DISTINCT FROM source.title
            OR target.description IS DISTINCT FROM source.description
            OR target.era IS DISTINCT FROM source.era
            OR target.condition IS DISTINCT FROM source.condition
            OR target.dimensions IS DISTINCT FROM source.dimensions
        ) THEN
            UPDATE SET
                shop_id = source.shop_id,
                category_id = source.category_id,
                maker_id = source.maker_id,
                material_id = source.material_id,
                title = source.title,
                description = source.description,
                era = source.era,
                condition = source.condition,
                dimensions = source.dimensions,
                execution_id = v_execution_id,
                batch_id = v_batch_id,
                ingested_at = CAST(CURRENT_TIMESTAMP() AS DATETIME)
        WHEN NOT MATCHED THEN
            INSERT (
                product_id, shop_id, category_id, maker_id, material_id,
                url, title, description, era, condition, dimensions,
                execution_id, batch_id, ingested_at
            )
            VALUES (
                source.product_id, source.shop_id, source.category_id, source.maker_id, source.material_id,
                source.url, source.title, source.description, source.era, source.condition, source.dimensions,
                v_execution_id, v_batch_id, CAST(CURRENT_TIMESTAMP() AS DATETIME)
            );

        -- STREAMING_CHUNK: Removing unused dimension members...
        -- Remove dimension rows no fact row points to any more (e.g. old keys from
        -- before case-insensitive matching, or values that no longer occur).
        -- Dimensions are fully derived from Silver, so nothing is lost.
        DELETE FROM statement_competitor_data_warehouse.dim_shop d
        WHERE NOT EXISTS (SELECT 1 FROM statement_competitor_data_warehouse.fact_product_scrape f WHERE f.shop_id = d.shop_id);

        DELETE FROM statement_competitor_data_warehouse.dim_category d
        WHERE NOT EXISTS (SELECT 1 FROM statement_competitor_data_warehouse.fact_product_scrape f WHERE f.category_id = d.category_id);

        DELETE FROM statement_competitor_data_warehouse.dim_maker d
        WHERE NOT EXISTS (SELECT 1 FROM statement_competitor_data_warehouse.fact_product_scrape f WHERE f.maker_id = d.maker_id);

        DELETE FROM statement_competitor_data_warehouse.dim_material d
        WHERE NOT EXISTS (SELECT 1 FROM statement_competitor_data_warehouse.fact_product_scrape f WHERE f.material_id = d.material_id);

        -- STREAMING_CHUNK: Catching execution failures and logging error details...
    EXCEPTION WHEN ERROR THEN
        SET v_error_msg = FORMAT('Message: %s | Statement: %s', @@error.message, @@error.statement_text);
        SET v_error_type = CASE
            WHEN LOWER(v_error_msg) LIKE '%merge%' THEN 'NORMALISATION_MERGE_FAILURE'
            WHEN LOWER(v_error_msg) LIKE '%fingerprint%' THEN 'HASH_GENERATION_FAILURE'
            ELSE 'GENERAL_NORMALISATION_FAILURE'
        END;

        INSERT INTO statement_competitor_data_warehouse.silver_normalised_error_logs (
            log_id, execution_id, batch_id, error_timestamp, procedure_name, error_type, error_details
        )
        VALUES (
            GENERATE_UUID(), v_execution_id, v_batch_id, CURRENT_TIMESTAMP(),
            'sp_normalise_silver_products', v_error_type, v_error_msg
        );

        RAISE USING MESSAGE = FORMAT('sp_normalise_silver_products failed [Execution ID: %s]: %s', v_execution_id, v_error_msg);
    END;

    -- STREAMING_CHUNK: Logging successful completion metrics...
    INSERT INTO statement_competitor_data_warehouse.silver_normalised_error_logs (
        log_id, execution_id, batch_id, error_timestamp, procedure_name, error_type, error_details
    )
    VALUES (
        GENERATE_UUID(), v_execution_id, v_batch_id, CURRENT_TIMESTAMP(),
        'sp_normalise_silver_products', 'INFO_STAGE_COMPLETE',
        FORMAT('Star Schema Normalisation COMPLETED in %d seconds.', TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), start_time, SECOND))
    );
END;

-- =============================================================================
-- INVOCATION & VERIFICATION
-- =============================================================================
-- CALL statement_competitor_data_warehouse.sp_normalise_silver_products();

-- Compare Source vs. Fact Table Row Counts
-- This ensures no data was unintentionally dropped during the merge into the fact table.

SELECT
    'statement_competitor_data_warehouse.silver_transformed_products' AS table_name,
    COUNT(*) AS row_count
FROM `statement_competitor_data_warehouse.silver_transformed_products`
UNION ALL
SELECT
    'Fact Table (fact_product_scrape)' AS table_name,
    COUNT(*) AS row_count
FROM `statement_competitor_data_warehouse.fact_product_scrape`;


-- Verify Dimension Table Population
-- This confirms that the FARM_FINGERPRINT and COALESCE logic successfully generated unique records for your dimensions.

SELECT 'dim_shop' AS dimension_table, COUNT(*) AS unique_records FROM `statement_competitor_data_warehouse.dim_shop`
UNION ALL
SELECT 'dim_category', COUNT(*) FROM `statement_competitor_data_warehouse.dim_category`
UNION ALL
SELECT 'dim_maker', COUNT(*) FROM `statement_competitor_data_warehouse.dim_maker`
UNION ALL
SELECT 'dim_material', COUNT(*) FROM `statement_competitor_data_warehouse.dim_material`;

-- Check for Unmapped Dimension Keys (Orphaned Facts)
-- This checks if there are any records in the fact table pointing to a surrogate key (like shop_id) that does not
-- exist in the corresponding dimension table. It challenges the assumption that the COALESCE logic caught absolutely everything.

SELECT
    COUNT(f.shop_id) AS missing_shop_ids
FROM `statement_competitor_data_warehouse.fact_product_scrape` f
LEFT JOIN `statement_competitor_data_warehouse.dim_shop` d
    ON f.shop_id = d.shop_id
WHERE d.shop_id IS NULL;