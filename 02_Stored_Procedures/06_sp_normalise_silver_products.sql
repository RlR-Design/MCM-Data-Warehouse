/*
===============================================================================
Script   : 06_sp_normalise_silver_products.sql
Layer    : Silver
Purpose  : Builds the star schema from silver_transformed_products:
           dim_shop, dim_category, dim_maker, dim_material and
           fact_product_scrape. Keys are FARM_FINGERPRINT hashes of the
           normalised value, so spelling variants share one key.
When     : Deploy at setup and after any change. Runs as step 6 of sp_run_pipeline.
Requires : 05_sp_validate_silver_products has completed.
Logs     : silver_normalised_error_logs
Warning  : Dimension rows not referenced by any fact row are deleted on every run.
===============================================================================
*/

CREATE OR REPLACE PROCEDURE statement_competitor_data_warehouse.sp_normalise_silver_products()
OPTIONS(strict_mode=false)
BEGIN
    DECLARE start_time TIMESTAMP;
    DECLARE v_execution_id STRING;
    DECLARE v_batch_id STRING;
    DECLARE v_error_msg STRING;
    DECLARE v_error_type STRING;

    -- Initialise run identifiers.
    SET start_time = CURRENT_TIMESTAMP();
    SET v_execution_id = GENERATE_UUID();
    SET v_batch_id = FORMAT_TIMESTAMP('BATCH_NORM_%Y%m%d_%H%M%S', start_time);

    -- Log start.
    INSERT INTO statement_competitor_data_warehouse.silver_normalised_error_logs (
        log_id, execution_id, batch_id, error_timestamp, procedure_name, error_type, error_details
    )
    VALUES (
        GENERATE_UUID(), v_execution_id, v_batch_id, CURRENT_TIMESTAMP(),
        'sp_normalise_silver_products', 'INFO_STAGE_START',
        'Starting Star Schema Normalisation: Extracting dimensions and generating INT64 Surrogate Keys via FARM_FINGERPRINT.'
    );

    BEGIN
        -- Stage Silver rows with normalised dimension keys (lower case, trimmed,
        -- single spaces), so "Teak", "teak" and "Teak " share one key.
        CREATE OR REPLACE TEMP TABLE stg_normalised AS
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

        -- Shop attributes: most common spelling, and most common known region, city
        -- and location per shop. Values are counted first, then the top one is taken.
        CREATE OR REPLACE TEMP TABLE stg_dim_shop AS
        WITH names AS (
            SELECT shop_key, shop_name AS v, COUNT(*) AS n
            FROM stg_normalised
            GROUP BY shop_key, shop_name
        ),
        regions AS (
            SELECT shop_key, region AS v, COUNT(*) AS n
            FROM stg_normalised
            WHERE region IS NOT NULL AND region != 'Unknown'
            GROUP BY shop_key, region
        ),
        cities AS (
            SELECT shop_key, city AS v, COUNT(*) AS n
            FROM stg_normalised
            WHERE city IS NOT NULL AND city != 'Unknown'
            GROUP BY shop_key, city
        ),
        locations AS (
            SELECT shop_key, latitude, longitude, COUNT(*) AS n
            FROM stg_normalised
            WHERE latitude IS NOT NULL AND longitude IS NOT NULL
            GROUP BY shop_key, latitude, longitude
        )
        SELECT
            FARM_FINGERPRINT(k.shop_key) AS shop_id,
            nm.shop_name,
            COALESCE(rg.region, 'Unknown') AS region,
            COALESCE(ct.city, 'Unknown') AS city,
            lc.location.latitude AS latitude,
            lc.location.longitude AS longitude
        FROM (SELECT DISTINCT shop_key FROM stg_normalised) AS k
        LEFT JOIN (
            SELECT shop_key, ARRAY_AGG(v ORDER BY n DESC, v LIMIT 1)[OFFSET(0)] AS shop_name
            FROM names GROUP BY shop_key
        ) AS nm ON nm.shop_key = k.shop_key
        LEFT JOIN (
            SELECT shop_key, ARRAY_AGG(v ORDER BY n DESC, v LIMIT 1)[OFFSET(0)] AS region
            FROM regions GROUP BY shop_key
        ) AS rg ON rg.shop_key = k.shop_key
        LEFT JOIN (
            SELECT shop_key, ARRAY_AGG(v ORDER BY n DESC, v LIMIT 1)[OFFSET(0)] AS city
            FROM cities GROUP BY shop_key
        ) AS ct ON ct.shop_key = k.shop_key
        LEFT JOIN (
            SELECT shop_key, ARRAY_AGG(STRUCT(latitude, longitude) ORDER BY n DESC, latitude, longitude LIMIT 1)[OFFSET(0)] AS location
            FROM locations GROUP BY shop_key
        ) AS lc ON lc.shop_key = k.shop_key;

        -- Merge dim_shop.
        MERGE statement_competitor_data_warehouse.dim_shop AS target
        USING stg_dim_shop AS source
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

        -- Merge dim_category: most common spelling per category.
        MERGE statement_competitor_data_warehouse.dim_category AS target
        USING (
            SELECT
                FARM_FINGERPRINT(category_key) AS category_id,
                ARRAY_AGG(category ORDER BY n DESC, category LIMIT 1)[OFFSET(0)] AS category
            FROM (
                SELECT category_key, category, COUNT(*) AS n
                FROM stg_normalised
                GROUP BY category_key, category
            )
            GROUP BY category_key
        ) AS source
        ON target.category_id = source.category_id
        WHEN MATCHED AND target.category IS DISTINCT FROM source.category THEN
            UPDATE SET category = source.category
        WHEN NOT MATCHED THEN
            INSERT (category_id, category)
            VALUES (source.category_id, source.category);

        -- Merge dim_maker: most common spelling per maker.
        MERGE statement_competitor_data_warehouse.dim_maker AS target
        USING (
            SELECT
                FARM_FINGERPRINT(maker_key) AS maker_id,
                ARRAY_AGG(makers ORDER BY n DESC, makers LIMIT 1)[OFFSET(0)] AS makers
            FROM (
                SELECT maker_key, makers, COUNT(*) AS n
                FROM stg_normalised
                GROUP BY maker_key, makers
            )
            GROUP BY maker_key
        ) AS source
        ON target.maker_id = source.maker_id
        WHEN MATCHED AND target.makers IS DISTINCT FROM source.makers THEN
            UPDATE SET makers = source.makers
        WHEN NOT MATCHED THEN
            INSERT (maker_id, makers)
            VALUES (source.maker_id, source.makers);

        -- Merge dim_material: most common spelling per material.
        MERGE statement_competitor_data_warehouse.dim_material AS target
        USING (
            SELECT
                FARM_FINGERPRINT(material_key) AS material_id,
                ARRAY_AGG(primary_material ORDER BY n DESC, primary_material LIMIT 1)[OFFSET(0)] AS primary_material
            FROM (
                SELECT material_key, primary_material, COUNT(*) AS n
                FROM stg_normalised
                GROUP BY material_key, primary_material
            )
            GROUP BY material_key
        ) AS source
        ON target.material_id = source.material_id
        WHEN MATCHED AND target.primary_material IS DISTINCT FROM source.primary_material THEN
            UPDATE SET primary_material = source.primary_material
        WHEN NOT MATCHED THEN
            INSERT (material_id, primary_material)
            VALUES (source.material_id, source.primary_material);

        -- Merge fact_product_scrape: one row per URL. Only changed rows are rewritten.
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
            -- Keep the most recent row per URL.
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

        -- Delete dimension rows no fact row references (derived from Silver, nothing is lost).
        DELETE FROM statement_competitor_data_warehouse.dim_shop d
        WHERE NOT EXISTS (SELECT 1 FROM statement_competitor_data_warehouse.fact_product_scrape f WHERE f.shop_id = d.shop_id);

        DELETE FROM statement_competitor_data_warehouse.dim_category d
        WHERE NOT EXISTS (SELECT 1 FROM statement_competitor_data_warehouse.fact_product_scrape f WHERE f.category_id = d.category_id);

        DELETE FROM statement_competitor_data_warehouse.dim_maker d
        WHERE NOT EXISTS (SELECT 1 FROM statement_competitor_data_warehouse.fact_product_scrape f WHERE f.maker_id = d.maker_id);

        DELETE FROM statement_competitor_data_warehouse.dim_material d
        WHERE NOT EXISTS (SELECT 1 FROM statement_competitor_data_warehouse.fact_product_scrape f WHERE f.material_id = d.material_id);

    EXCEPTION WHEN ERROR THEN
        -- Capture and classify the error.
        SET v_error_msg = FORMAT('Message: %s | Statement: %s', @@error.message, @@error.statement_text);
        SET v_error_type = CASE
            WHEN LOWER(v_error_msg) LIKE '%merge%' THEN 'NORMALISATION_MERGE_FAILURE'
            WHEN LOWER(v_error_msg) LIKE '%fingerprint%' THEN 'HASH_GENERATION_FAILURE'
            ELSE 'GENERAL_NORMALISATION_FAILURE'
        END;

        -- Log the error.
        INSERT INTO statement_competitor_data_warehouse.silver_normalised_error_logs (
            log_id, execution_id, batch_id, error_timestamp, procedure_name, error_type, error_details
        )
        VALUES (
            GENERATE_UUID(), v_execution_id, v_batch_id, CURRENT_TIMESTAMP(),
            'sp_normalise_silver_products', v_error_type, v_error_msg
        );

        -- Re-raise so the caller sees the failure.
        RAISE USING MESSAGE = FORMAT('sp_normalise_silver_products failed [Execution ID: %s]: %s', v_execution_id, v_error_msg);
    END;

    -- Log completion.
    INSERT INTO statement_competitor_data_warehouse.silver_normalised_error_logs (
        log_id, execution_id, batch_id, error_timestamp, procedure_name, error_type, error_details
    )
    VALUES (
        GENERATE_UUID(), v_execution_id, v_batch_id, CURRENT_TIMESTAMP(),
        'sp_normalise_silver_products', 'INFO_STAGE_COMPLETE',
        FORMAT('Star Schema Normalisation COMPLETED in %d seconds.', TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), start_time, SECOND))
    );
END;


/*
===============================================================================
Tests (run manually)
===============================================================================
-- Rebuild the star schema.
CALL statement_competitor_data_warehouse.sp_normalise_silver_products();

-- Fact covers every Silver product (expect equal counts).
SELECT
    (SELECT COUNT(*) FROM statement_competitor_data_warehouse.silver_transformed_products) AS silver_rows,
    (SELECT COUNT(*) FROM statement_competitor_data_warehouse.fact_product_scrape) AS fact_rows;

-- Dimension sizes.
SELECT 'dim_shop' AS dimension, COUNT(*) AS members FROM statement_competitor_data_warehouse.dim_shop
UNION ALL SELECT 'dim_category', COUNT(*) FROM statement_competitor_data_warehouse.dim_category
UNION ALL SELECT 'dim_maker', COUNT(*) FROM statement_competitor_data_warehouse.dim_maker
UNION ALL SELECT 'dim_material', COUNT(*) FROM statement_competitor_data_warehouse.dim_material;

-- Orphaned fact keys per dimension (expect all 0).
SELECT
    COUNTIF(s.shop_id IS NULL) AS missing_shop,
    COUNTIF(c.category_id IS NULL) AS missing_category,
    COUNTIF(m.maker_id IS NULL) AS missing_maker,
    COUNTIF(mat.material_id IS NULL) AS missing_material
FROM statement_competitor_data_warehouse.fact_product_scrape f
LEFT JOIN statement_competitor_data_warehouse.dim_shop s ON s.shop_id = f.shop_id
LEFT JOIN statement_competitor_data_warehouse.dim_category c ON c.category_id = f.category_id
LEFT JOIN statement_competitor_data_warehouse.dim_maker m ON m.maker_id = f.maker_id
LEFT JOIN statement_competitor_data_warehouse.dim_material mat ON mat.material_id = f.material_id;

-- Case or spacing variants left in a dimension (expect no rows).
SELECT LOWER(REGEXP_REPLACE(TRIM(primary_material), r'\s+', ' ')) AS material_key, COUNT(*) AS n
FROM statement_competitor_data_warehouse.dim_material
GROUP BY material_key
HAVING COUNT(*) > 1;
*/
