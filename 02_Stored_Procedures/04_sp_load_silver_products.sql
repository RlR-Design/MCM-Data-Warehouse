/*
===============================================================================
Script   : 04_sp_load_silver_products.sql
Layer    : Silver
Purpose  : Cleans and types one validated Bronze batch and merges it into
           silver_transformed_products. Only new or changed products are
           written. Bronze-quarantined rows are skipped. Missing shop names
           and unparseable prices are left NULL for Silver validation.
When     : Deploy at setup and after any change. Runs as step 4 of sp_run_pipeline.
Requires : 02_sp_validate_bronze_products has validated the batch.
           ref_shop_locations (04_create_reference_tables.sql).
Params   : p_bronze_batch_id  Bronze batch to load. NULL = latest validated batch.
Logs     : silver_transformed_error_logs
Warning  : sp_reconciled_silver_products reads the Bronze batch id from the
           completion log text. Keep the wording "for Bronze batch <id>.".
===============================================================================
*/

CREATE OR REPLACE PROCEDURE statement_competitor_data_warehouse.sp_load_silver_products (IN p_bronze_batch_id STRING)
OPTIONS(strict_mode=false)
BEGIN
    DECLARE start_time TIMESTAMP;
    DECLARE v_execution_id STRING;
    DECLARE v_batch_id STRING;
    DECLARE v_bronze_batch_id STRING;
    DECLARE v_rows_merged INT64 DEFAULT 0;
    DECLARE v_error_msg STRING;
    DECLARE v_error_type STRING;

    -- Initialise run identifiers.
    SET start_time = CURRENT_TIMESTAMP();
    SET v_execution_id = GENERATE_UUID();
    SET v_batch_id = FORMAT_TIMESTAMP('BATCH_SLV_%Y%m%d_%H%M%S', start_time);

    -- Resolve the Bronze batch: the parameter, else the latest validated batch.
    SET v_bronze_batch_id = COALESCE(
        p_bronze_batch_id,
        (
            SELECT batch_id
            FROM statement_competitor_data_warehouse.bronze_validity_log
            ORDER BY validation_timestamp DESC
            LIMIT 1
        )
    );

    -- Log start.
    INSERT INTO statement_competitor_data_warehouse.silver_transformed_error_logs (
        log_id, execution_id, batch_id, error_timestamp, scraper_job_id, error_type, error_details
    )
    VALUES (
        GENERATE_UUID(), v_execution_id, v_batch_id, CURRENT_TIMESTAMP(),
        'sp_load_silver_products', 'INFO_STAGE_START',
        FORMAT('Starting Silver Layer Transformation and MERGE for Bronze batch %s, excluding Bronze-quarantined records.', COALESCE(v_bronze_batch_id, 'NONE'))
    );

    BEGIN
        -- Stop unless the batch has been through Bronze validation.
        IF v_bronze_batch_id IS NULL OR NOT EXISTS (
            SELECT 1
            FROM statement_competitor_data_warehouse.bronze_validity_log
            WHERE batch_id = v_bronze_batch_id
        ) THEN
            RAISE USING MESSAGE = FORMAT('Bronze batch %s has not been validated. Run sp_validate_bronze_products first.', COALESCE(v_bronze_batch_id, 'NONE'));
        END IF;

        -- Merge the cleaned batch into Silver.
        MERGE statement_competitor_data_warehouse.silver_transformed_products AS target
        USING (
            -- Clean and type each Bronze column.
            WITH parsed_source AS (
                SELECT
                    -- Shop name: mandatory, NULL when missing (quarantined by validation).
                    CASE
                        WHEN raw_shop_name IS NULL OR TRIM(raw_shop_name) = '' OR UPPER(TRIM(raw_shop_name)) IN ('NULL', 'NONE', 'NAN', 'N/A', '-', 'UNKNOWN') THEN NULL
                        ELSE TRIM(raw_shop_name)
                    END AS shop_name,

                    -- Region: 'Unknown' when missing.
                    CASE
                        WHEN raw_region IS NULL OR TRIM(raw_region) = '' OR UPPER(TRIM(raw_region)) IN ('NULL', 'NONE', 'NAN', 'N/A', '-', 'UNKNOWN') THEN 'Unknown'
                        ELSE TRIM(raw_region)
                    END AS region,

                    -- City: 'Unknown' when missing.
                    CASE
                        WHEN raw_city IS NULL OR TRIM(raw_city) = '' OR UPPER(TRIM(raw_city)) IN ('NULL', 'NONE', 'NAN', 'N/A', '-', 'UNKNOWN') THEN 'Unknown'
                        ELSE TRIM(raw_city)
                    END AS city,

                    -- Title: 'Unknown' when missing.
                    CASE
                        WHEN raw_title IS NULL OR TRIM(raw_title) = '' OR UPPER(TRIM(raw_title)) IN ('NULL', 'NONE', 'NAN', 'N/A', '-') THEN 'Unknown'
                        ELSE TRIM(raw_title)
                    END AS title,

                    -- Description: '-1' when missing.
                    CASE
                        WHEN raw_description IS NULL OR TRIM(raw_description) = '' OR UPPER(TRIM(raw_description)) IN ('NULL', 'NONE', 'NAN', 'N/A', '-') THEN '-1'
                        ELSE TRIM(raw_description)
                    END AS description,

                    -- Dimensions: '-1' when missing.
                    CASE
                        WHEN raw_dimension IS NULL OR TRIM(raw_dimension) = '' OR UPPER(TRIM(raw_dimension)) IN ('NULL', 'NONE', 'NAN', 'N/A', '-') THEN '-1'
                        ELSE TRIM(raw_dimension)
                    END AS dimensions,

                    -- URL: natural key.
                    TRIM(raw_url) AS url,

                    -- Category: 'Uncategorised' when missing.
                    CASE
                        WHEN raw_category IS NULL OR TRIM(raw_category) = '' OR UPPER(TRIM(raw_category)) IN ('NULL', 'NONE', 'NAN', 'N/A', '-', 'UNCATEGORISED') THEN 'Uncategorised'
                        ELSE TRIM(raw_category)
                    END AS category,

                    -- Makers: '-1' when missing.
                    CASE
                        WHEN raw_makers IS NULL OR TRIM(raw_makers) = '' OR UPPER(TRIM(raw_makers)) IN ('NULL', 'NONE', 'NAN', 'N/A', '-') THEN '-1'
                        ELSE TRIM(raw_makers)
                    END AS makers,

                    -- Primary material: '-1' when missing.
                    CASE
                        WHEN raw_primary_material IS NULL OR TRIM(raw_primary_material) = '' OR UPPER(TRIM(raw_primary_material)) IN ('NULL', 'NONE', 'NAN', 'N/A', '-') THEN '-1'
                        ELSE TRIM(raw_primary_material)
                    END AS primary_material,

                    -- Era: '-1' when missing.
                    CASE
                        WHEN raw_era IS NULL OR TRIM(raw_era) = '' OR UPPER(TRIM(raw_era)) IN ('NULL', 'NONE', 'NAN', 'N/A', '-') THEN '-1'
                        ELSE TRIM(raw_era)
                    END AS era,

                    -- Condition: '-1' when missing.
                    CASE
                        WHEN raw_condition IS NULL OR TRIM(raw_condition) = '' OR UPPER(TRIM(raw_condition)) IN ('NULL', 'NONE', 'NAN', 'N/A', '-') THEN '-1'
                        ELSE TRIM(raw_condition)
                    END AS condition,

                    -- Latitude: NULL when missing, unparseable, NaN or outside -90..90.
                    IF(
                        SAFE_CAST(TRIM(raw_latitude) AS FLOAT64) BETWEEN -90.0 AND 90.0,
                        SAFE_CAST(TRIM(raw_latitude) AS FLOAT64),
                        NULL
                    ) AS latitude,

                    -- Longitude: NULL when missing, unparseable, NaN or outside -180..180.
                    IF(
                        SAFE_CAST(TRIM(raw_longitude) AS FLOAT64) BETWEEN -180.0 AND 180.0,
                        SAFE_CAST(TRIM(raw_longitude) AS FLOAT64),
                        NULL
                    ) AS longitude,

                    -- Price: digits, '.' and '-' kept, rounded to 2 decimals. NULL when unparseable.
                    ROUND(
                        SAFE_CAST(
                            REGEXP_REPLACE(TRIM(raw_price), r'[^0-9.-]', '') AS NUMERIC
                        ),
                        2
                    ) AS price,

                    -- Available: common TRUE/FALSE spellings. NULL (unknown) otherwise, never assumed sold.
                    CASE
                        WHEN UPPER(TRIM(raw_available)) IN ('TRUE', 'T', 'YES', 'Y', '1', '1.0', 'AVAILABLE', 'IN STOCK', 'INSTOCK', 'IN_STOCK', 'FOR SALE') THEN TRUE
                        WHEN UPPER(TRIM(raw_available)) IN ('FALSE', 'F', 'NO', 'N', '0', '0.0', 'SOLD', 'SOLD OUT', 'SOLDOUT', 'SOLD_OUT', 'OUT OF STOCK', 'OUTOFSTOCK', 'OUT_OF_STOCK', 'UNAVAILABLE', 'NOT AVAILABLE') THEN FALSE
                        ELSE NULL
                    END AS available,

                    -- Scrape time: NULL when unparseable, never replaced with the run time.
                    COALESCE(
                        SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%E*S', TRIM(scrape_timestamp)),
                        SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*S', TRIM(scrape_timestamp)),
                        SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%SZ', TRIM(scrape_timestamp)),
                        SAFE_CAST(TRIM(scrape_timestamp) AS TIMESTAMP)
                    ) AS parsed_ts

                FROM statement_competitor_data_warehouse.bronze_raw_products
                WHERE batch_id = v_bronze_batch_id
                  AND raw_url IS NOT NULL
                  AND TRIM(raw_url) != ''
                  AND UPPER(TRIM(raw_url)) NOT IN ('NULL', 'NONE', 'NAN', 'N/A', '-')
                  -- Skip Bronze-quarantined rows (same price rule as sp_validate_bronze_products).
                  AND SAFE_CAST(REGEXP_REPLACE(raw_price, r'[^0-9.-]', '') AS FLOAT64) IS NOT NULL
            ),
            -- Verified shop coordinates.
            shop_lookup AS (
                SELECT
                    TRIM(shop_name) AS lookup_shop,
                    latitude AS lookup_lat,
                    longitude AS lookup_lon
                FROM statement_competitor_data_warehouse.ref_shop_locations
            )
            -- Apply verified coordinates and keep the latest row per URL.
            SELECT
                p.shop_name,
                COALESCE(l.lookup_lat, p.latitude) AS latitude,
                COALESCE(l.lookup_lon, p.longitude) AS longitude,
                p.region,
                p.city,
                p.category,
                p.title,
                p.price,
                p.description,
                p.makers,
                p.primary_material,
                p.era,
                p.condition,
                p.dimensions,
                p.url,
                p.available,
                DATE(p.parsed_ts) AS timestamp_date,
                TIME(p.parsed_ts) AS timestamp_time
            FROM parsed_source p
            LEFT JOIN shop_lookup l ON LOWER(p.shop_name) = LOWER(l.lookup_shop)
            QUALIFY ROW_NUMBER() OVER (PARTITION BY p.url ORDER BY p.parsed_ts DESC) = 1
        ) AS source
        ON target.url = source.url

        -- Update only when product data changed (a new scrape time alone is not a change).
        WHEN MATCHED AND (
               target.shop_name IS DISTINCT FROM source.shop_name
            OR target.latitude IS DISTINCT FROM source.latitude
            OR target.longitude IS DISTINCT FROM source.longitude
            OR target.region IS DISTINCT FROM source.region
            OR target.city IS DISTINCT FROM source.city
            OR target.category IS DISTINCT FROM source.category
            OR target.title IS DISTINCT FROM source.title
            OR target.price IS DISTINCT FROM source.price
            OR target.description IS DISTINCT FROM source.description
            OR target.makers IS DISTINCT FROM source.makers
            OR target.primary_material IS DISTINCT FROM source.primary_material
            OR target.era IS DISTINCT FROM source.era
            OR target.condition IS DISTINCT FROM source.condition
            OR target.dimensions IS DISTINCT FROM source.dimensions
            OR target.available IS DISTINCT FROM source.available
        ) THEN
            UPDATE SET
                shop_name = source.shop_name,
                latitude = source.latitude,
                longitude = source.longitude,
                region = source.region,
                city = source.city,
                category = source.category,
                title = source.title,
                price = source.price,
                description = source.description,
                makers = source.makers,
                primary_material = source.primary_material,
                era = source.era,
                condition = source.condition,
                dimensions = source.dimensions,
                available = source.available,
                timestamp_date = source.timestamp_date,
                timestamp_time = source.timestamp_time,
                execution_id = v_execution_id,
                batch_id = v_batch_id,
                ingested_at = CAST(CURRENT_TIMESTAMP() AS DATETIME)

        -- Insert new products.
        WHEN NOT MATCHED THEN
            INSERT (
                shop_name, latitude, longitude, region, city, category,
                title, price, description, makers, primary_material,
                era, condition, dimensions, url, available,
                timestamp_date, timestamp_time,
                execution_id, batch_id, ingested_at
            )
            VALUES (
                source.shop_name, source.latitude, source.longitude, source.region,
                source.city, source.category, source.title, source.price,
                source.description, source.makers, source.primary_material,
                source.era, source.condition, source.dimensions, source.url,
                source.available, source.timestamp_date, source.timestamp_time,
                v_execution_id, v_batch_id, CAST(CURRENT_TIMESTAMP() AS DATETIME)
            );

        SET v_rows_merged = @@row_count;

    EXCEPTION WHEN ERROR THEN
        -- Capture and classify the error.
        SET v_error_msg = FORMAT('Message: %s | Statement: %s', @@error.message, @@error.statement_text);

        SET v_error_type = CASE
            WHEN LOWER(v_error_msg) LIKE '%cast%' OR LOWER(v_error_msg) LIKE '%type%' THEN 'CASTING_FAILURE'
            WHEN LOWER(v_error_msg) LIKE '%merge%' OR LOWER(v_error_msg) LIKE '%duplicate%' THEN 'MERGE_FAILURE'
            ELSE 'TRANSFORMATION_FAILURE'
        END;

        -- Log the error.
        INSERT INTO statement_competitor_data_warehouse.silver_transformed_error_logs (
            log_id, execution_id, batch_id, error_timestamp, scraper_job_id, error_type, error_details
        )
        VALUES (
            GENERATE_UUID(), v_execution_id, v_batch_id, CURRENT_TIMESTAMP(),
            'sp_load_silver_products', v_error_type, v_error_msg
        );

        -- Re-raise so the caller sees the failure.
        RAISE USING MESSAGE = FORMAT('sp_load_silver_products failed [Execution ID: %s]: %s', v_execution_id, v_error_msg);
    END;

    -- Log completion. Wording is parsed by sp_reconciled_silver_products.
    INSERT INTO statement_competitor_data_warehouse.silver_transformed_error_logs (
        log_id, execution_id, batch_id, error_timestamp, scraper_job_id, error_type, error_details
    )
    VALUES (
        GENERATE_UUID(), v_execution_id, v_batch_id, CURRENT_TIMESTAMP(),
        'sp_load_silver_products', 'INFO_STAGE_COMPLETE',
        FORMAT('Transformation COMPLETED for Bronze batch %s. Rows inserted or changed: %d in %d seconds',
               v_bronze_batch_id, v_rows_merged, TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), start_time, SECOND))
    );
END;


/*
===============================================================================
Tests (run manually)
===============================================================================
-- Load the latest validated Bronze batch.
CALL statement_competitor_data_warehouse.sp_load_silver_products(NULL);

-- Load a specific Bronze batch.
CALL statement_competitor_data_warehouse.sp_load_silver_products('BATCH_YYYYMMDD_HHMMSS');

-- Latest load result (Bronze batch and rows inserted or changed).
SELECT error_timestamp, error_type, error_details
FROM statement_competitor_data_warehouse.silver_transformed_error_logs
WHERE scraper_job_id = 'sp_load_silver_products'
ORDER BY error_timestamp DESC
LIMIT 5;

-- One row per URL (expect total_rows = unique_urls).
SELECT COUNT(*) AS total_rows, COUNT(DISTINCT url) AS unique_urls
FROM statement_competitor_data_warehouse.silver_transformed_products;

-- Rows awaiting Silver validation (missing shop or price).
SELECT COUNTIF(shop_name IS NULL) AS missing_shop, COUNTIF(price IS NULL) AS missing_price
FROM statement_competitor_data_warehouse.silver_transformed_products;

-- Unknown availability values (add new spellings to the CASE above).
SELECT url, title
FROM statement_competitor_data_warehouse.silver_transformed_products
WHERE available IS NULL;

-- Known shops use verified coordinates (expect no rows).
SELECT s.shop_name, s.latitude, r.latitude AS ref_latitude
FROM statement_competitor_data_warehouse.silver_transformed_products s
JOIN statement_competitor_data_warehouse.ref_shop_locations r ON LOWER(r.shop_name) = LOWER(s.shop_name)
WHERE s.latitude IS DISTINCT FROM r.latitude;
*/
