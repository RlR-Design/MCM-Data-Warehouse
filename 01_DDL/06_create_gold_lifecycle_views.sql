/*
===============================================================================
Script   : 06_create_gold_lifecycle_views.sql
Layer    : Gold
Purpose  : Works out what happened to each competitor product (listed,
           discounted, sold, delisted, relisted) from the pipeline's raw facts:
             bronze_raw_products  presence in each shop scrape (full snapshots)
             bronze_scrape_health FAILED scrapes, excluded from delisting
             dim_price_history    price and availability changes
             ref_shop_rules       how each shop signals a sale
           Creates, in dependency order:
             gold_product_presence  one row per product per shop scrape
             gold_product_events    one row per lifecycle event
             gold_product_status    one row per product with current status
             gold_products_flat     one row per product, all attributes (Data Studio source)
When     : Initial setup, after 05_create_gold_views.sql.
           Re-run after any change to this file. Safe to re-run.
Warning  : Views hold no data. gold_product_presence scans all of Bronze; if it
           becomes slow, materialise it as a table refreshed by a procedure.
===============================================================================
*/

-- -----------------------------------------------------------------------------
-- gold_product_presence: one row per product per scrape of its shop, from the
-- product's first sighting onwards. is_present = product was in that scrape.
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW statement_competitor_data_warehouse.gold_product_presence
OPTIONS(description="One row per product per scrape of its shop (from first sighting), showing whether the product was present")
AS
-- Valid Bronze rows with parsed scrape time (test batches excluded)
WITH bronze AS (
    SELECT
        TRIM(raw_shop_name) AS shop_name,
        TRIM(raw_url) AS url,
        batch_id,
        ingested_at,
        -- Same timestamp formats as sp_load_silver_products
        COALESCE(
            SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%E*S', TRIM(scrape_timestamp)),
            SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*S', TRIM(scrape_timestamp)),
            SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%SZ', TRIM(scrape_timestamp)),
            SAFE_CAST(TRIM(scrape_timestamp) AS TIMESTAMP)
        ) AS scrape_ts
    FROM statement_competitor_data_warehouse.bronze_raw_products
    WHERE batch_id IS NOT NULL
      AND NOT STARTS_WITH(batch_id, 'TEST_')
      AND NULLIF(TRIM(raw_url), '') IS NOT NULL
      AND UPPER(TRIM(raw_url)) NOT IN ('NULL', 'NONE', 'NAN', 'N/A', '-')
      AND NULLIF(TRIM(raw_shop_name), '') IS NOT NULL
      AND UPPER(TRIM(raw_shop_name)) NOT IN ('NULL', 'NONE', 'NAN', 'N/A', '-', 'UNKNOWN')
),
-- One scrape per shop per Bronze batch. Scrapes marked FAILED by
-- sp_check_scrape_health are ignored, so they never count as missed.
shop_batches AS (
    SELECT
        shop_name,
        batch_id,
        COALESCE(MAX(scrape_ts), MIN(ingested_at)) AS scrape_time,
        MIN(ingested_at) AS ingested_at
    FROM bronze b
    WHERE NOT EXISTS (
        SELECT 1
        FROM statement_competitor_data_warehouse.bronze_scrape_health h
        WHERE h.batch_id = b.batch_id
          AND h.shop_name = b.shop_name
          AND h.status = 'FAILED'
    )
    GROUP BY shop_name, batch_id
),
-- A batch that repeats an earlier scrape of the same shop (same scrape time)
-- is a duplicate load and is ignored
unique_shop_batches AS (
    SELECT *
    FROM shop_batches
    QUALIFY ROW_NUMBER() OVER (PARTITION BY shop_name, scrape_time ORDER BY ingested_at) = 1
),
-- Number each shop's scrapes in time order (no gaps, so "next scrape" = seq + 1)
shop_scrapes AS (
    SELECT
        shop_name,
        batch_id,
        scrape_time,
        ROW_NUMBER() OVER (PARTITION BY shop_name ORDER BY scrape_time, ingested_at) AS scrape_seq
    FROM unique_shop_batches
),
-- Products present in each shop scrape
sightings AS (
    SELECT DISTINCT b.shop_name, b.url, s.scrape_seq
    FROM bronze b
    JOIN shop_scrapes s ON s.shop_name = b.shop_name AND s.batch_id = b.batch_id
),
-- First scrape each product appeared in
first_sighting AS (
    SELECT shop_name, url, MIN(scrape_seq) AS first_seq
    FROM sightings
    GROUP BY shop_name, url
)
SELECT
    FARM_FINGERPRINT(f.url) AS product_id,
    f.url,
    f.shop_name,
    s.scrape_seq,
    s.batch_id,
    s.scrape_time,
    x.url IS NOT NULL AS is_present
FROM first_sighting f
JOIN shop_scrapes s
    ON s.shop_name = f.shop_name
   AND s.scrape_seq >= f.first_seq
LEFT JOIN sightings x
    ON x.shop_name = f.shop_name
   AND x.url = f.url
   AND x.scrape_seq = s.scrape_seq;


-- -----------------------------------------------------------------------------
-- gold_product_events: one row per lifecycle event
--   NEW_LISTING        first scrape containing the product
--   MARKDOWN           price dropped, still above zero
--   PRICE_INCREASE     price went up
--   SOLD_MARKED        site changed availability to sold
--   SOLD_ZERO_PRICE    price set to R0.00 where the shop rule means sold
--   PRICE_SET_TO_ZERO  price set to R0.00 where the shop rule does not mean sold
--   DELISTED           missing from delist_grace_scrapes consecutive shop scrapes
--   RELISTED           back after being delisted, marked sold or zero-priced
--   SCRAPE_GAP         missed fewer scrapes than the grace period, then seen again
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW statement_competitor_data_warehouse.gold_product_events
OPTIONS(description="One row per lifecycle event per product (listing, markdowns, sold signals, delisting, relisting)")
AS
-- Product presence per shop scrape
WITH presence AS (
    SELECT * FROM statement_competitor_data_warehouse.gold_product_presence
),
-- URL and shop per product
product_shop AS (
    SELECT product_id, ANY_VALUE(url) AS url, ANY_VALUE(shop_name) AS shop_name
    FROM presence
    GROUP BY product_id
),
-- Runs of consecutive present / absent scrapes per product (gaps-and-islands)
runs AS (
    SELECT
        product_id,
        url,
        shop_name,
        is_present,
        MIN(scrape_seq) AS run_start_seq,
        MAX(scrape_seq) AS run_end_seq,
        COUNT(*) AS run_length,
        MIN(scrape_time) AS run_start_time
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY scrape_seq)
              - ROW_NUMBER() OVER (PARTITION BY product_id, is_present ORDER BY scrape_seq) AS run_group
        FROM presence
    )
    GROUP BY product_id, url, shop_name, is_present, run_group
),
-- Absence runs with the shop's grace period and the time the product reappeared
absence_runs AS (
    SELECT
        r.*,
        COALESCE(sr.delist_grace_scrapes, dr.delist_grace_scrapes, 2) AS grace,
        next_scrape.scrape_time AS seen_again_at
    FROM runs r
    LEFT JOIN statement_competitor_data_warehouse.ref_shop_rules sr ON LOWER(sr.shop_name) = LOWER(r.shop_name)
    LEFT JOIN statement_competitor_data_warehouse.ref_shop_rules dr ON dr.shop_name = '*'
    -- The scrape straight after the absence run (if any) is where the product reappeared
    LEFT JOIN presence next_scrape
        ON next_scrape.product_id = r.product_id
       AND next_scrape.scrape_seq = r.run_end_seq + 1
    WHERE NOT r.is_present
),
-- Price and availability changes from the SCD2 history
history AS (
    SELECT
        h.product_id,
        ps.url,
        ps.shop_name,
        h.price,
        LAG(h.price) OVER w AS prev_price,
        h.available,
        LAG(h.available) OVER w AS prev_available,
        COALESCE(h.scrape_timestamp, h.valid_from) AS event_time,
        COALESCE(sr.zero_price_means_sold, dr.zero_price_means_sold, TRUE) AS zero_price_means_sold
    FROM statement_competitor_data_warehouse.dim_price_history h
    LEFT JOIN product_shop ps ON ps.product_id = h.product_id
    LEFT JOIN statement_competitor_data_warehouse.ref_shop_rules sr ON LOWER(sr.shop_name) = LOWER(ps.shop_name)
    LEFT JOIN statement_competitor_data_warehouse.ref_shop_rules dr ON dr.shop_name = '*'
    WINDOW w AS (PARTITION BY h.product_id ORDER BY h.valid_from)
)

-- First sighting
SELECT
    product_id, url, shop_name,
    'NEW_LISTING' AS event_type,
    MIN(scrape_time) AS event_time,
    CAST(NULL AS NUMERIC) AS price,
    CAST(NULL AS NUMERIC) AS previous_price,
    CAST(NULL AS FLOAT64) AS price_change_pct,
    'First scrape containing this product' AS detail
FROM presence
GROUP BY product_id, url, shop_name

UNION ALL

-- Delisted: missing long enough to pass the grace period
SELECT
    product_id, url, shop_name,
    'DELISTED',
    run_start_time,
    NULL, NULL, NULL,
    FORMAT('Missing from %d consecutive scrape(s) of %s (grace: %d)', run_length, shop_name, grace)
FROM absence_runs
WHERE run_length >= grace

UNION ALL

-- Relisted after being delisted, or a short gap that was probably a scraper miss
SELECT
    product_id, url, shop_name,
    IF(run_length >= grace, 'RELISTED', 'SCRAPE_GAP'),
    seen_again_at,
    NULL, NULL, NULL,
    FORMAT('Seen again after missing %d scrape(s)', run_length)
FROM absence_runs
WHERE seen_again_at IS NOT NULL

UNION ALL

-- Price / availability changes. One history row can produce more than one event.
SELECT
    product_id, url, shop_name,
    event_type,
    event_time,
    price,
    prev_price,
    IF(event_type IN ('MARKDOWN', 'PRICE_INCREASE'), CAST(ROUND(SAFE_DIVIDE(price - prev_price, prev_price) * 100, 1) AS FLOAT64), NULL),
    CASE event_type
        WHEN 'SOLD_MARKED' THEN 'Site changed availability to sold'
        WHEN 'SOLD_ZERO_PRICE' THEN 'Price changed to R0.00 (shop rule: zero price means sold)'
        WHEN 'PRICE_SET_TO_ZERO' THEN 'Price changed to R0.00 (shop rule: zero price does not mean sold)'
        WHEN 'RELISTED' THEN 'Available again after being marked sold or zero-priced'
        ELSE NULL
    END
FROM history,
UNNEST([
    IF(prev_available IS TRUE AND available IS FALSE, 'SOLD_MARKED', NULL),
    IF(prev_available IS FALSE AND available IS TRUE, 'RELISTED', NULL),
    CASE
        WHEN prev_price > 0 AND price = 0 THEN IF(zero_price_means_sold, 'SOLD_ZERO_PRICE', 'PRICE_SET_TO_ZERO')
        WHEN prev_price = 0 AND price > 0 THEN IF(zero_price_means_sold, 'RELISTED', 'PRICE_INCREASE')
        WHEN price < prev_price THEN 'MARKDOWN'
        WHEN price > prev_price THEN 'PRICE_INCREASE'
    END
]) AS event_type
WHERE event_type IS NOT NULL;


-- -----------------------------------------------------------------------------
-- gold_product_status: one row per product with its current status
--   ACTIVE             listed, not discounted
--   ACTIVE_DISCOUNTED  listed, below the first listed price
--   PENDING_DELIST     missing from recent scrapes, within the grace period
--   SOLD               delisted, marked sold or zero-priced (see sold_reason)
--   QUARANTINED        scraped, but never passed validation (see quarantine_reason)
-- sold_price is the last non-zero price, so R0.00 never skews averages.
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW statement_competitor_data_warehouse.gold_product_status
OPTIONS(description="One row per product with its current inferred status, sold date/price, markdowns and days on market")
AS
-- First/last seen and scrapes missed per product
WITH presence_summary AS (
    SELECT
        product_id,
        ANY_VALUE(url) AS url,
        ANY_VALUE(shop_name) AS shop_name,
        MIN(scrape_time) AS first_seen_at,
        MAX(IF(is_present, scrape_time, NULL)) AS last_seen_at,
        MAX(IF(is_present, scrape_seq, NULL)) AS last_present_seq,
        MAX(scrape_seq) AS latest_shop_seq
    FROM statement_competitor_data_warehouse.gold_product_presence
    GROUP BY product_id
),
-- Current state and first/last non-zero price per product
history_summary AS (
    SELECT
        product_id,
        ARRAY_AGG(
            STRUCT(price, available, COALESCE(scrape_timestamp, valid_from) AS since)
            ORDER BY valid_from DESC LIMIT 1
        )[OFFSET(0)] AS current_state,
        ARRAY_AGG(IF(price > 0, price, NULL) IGNORE NULLS ORDER BY valid_from ASC LIMIT 1)[SAFE_OFFSET(0)] AS first_listed_price,
        ARRAY_AGG(IF(price > 0, price, NULL) IGNORE NULLS ORDER BY valid_from DESC LIMIT 1)[SAFE_OFFSET(0)] AS last_listed_price
    FROM statement_competitor_data_warehouse.dim_price_history
    GROUP BY product_id
),
-- Latest quarantine reason per URL (Bronze and Silver validation)
quarantine_summary AS (
    SELECT url, ARRAY_AGG(error_reason ORDER BY quarantine_timestamp DESC LIMIT 1)[OFFSET(0)] AS quarantine_reason
    FROM (
        SELECT TRIM(JSON_VALUE(raw_payload, '$.raw_url')) AS url, error_reason, quarantine_timestamp
        FROM statement_competitor_data_warehouse.bronze_quarantine_bad_records
        UNION ALL
        SELECT TRIM(JSON_VALUE(raw_payload, '$.url')), error_reason, quarantine_timestamp
        FROM statement_competitor_data_warehouse.silver_quarantine_bad_records
    )
    WHERE url IS NOT NULL
    GROUP BY url
),
-- Markdown count and latest sold signal per product
event_summary AS (
    SELECT
        product_id,
        COUNTIF(event_type = 'MARKDOWN') AS markdown_count,
        MAX(IF(event_type IN ('SOLD_MARKED', 'SOLD_ZERO_PRICE'), event_time, NULL)) AS last_sold_signal_at
    FROM statement_competitor_data_warehouse.gold_product_events
    GROUP BY product_id
),
-- All inputs per product, with shop rules applied
combined AS (
    SELECT
        p.product_id,
        p.url,
        p.shop_name,
        p.first_seen_at,
        p.last_seen_at,
        p.latest_shop_seq - p.last_present_seq AS scrapes_missed,
        COALESCE(sr.delist_grace_scrapes, dr.delist_grace_scrapes, 2) AS grace,
        COALESCE(sr.zero_price_means_sold, dr.zero_price_means_sold, TRUE) AS zero_price_means_sold,
        first_missed.scrape_time AS first_missed_at,
        h.current_state.price AS current_price,
        h.current_state.available AS current_available,
        h.current_state.since AS current_state_since,
        h.first_listed_price,
        h.last_listed_price,
        COALESCE(e.markdown_count, 0) AS markdown_count,
        e.last_sold_signal_at,
        -- No price history: the product never passed validation.
        h.product_id IS NULL AS is_quarantined,
        q.quarantine_reason
    FROM presence_summary p
    LEFT JOIN history_summary h ON h.product_id = p.product_id
    LEFT JOIN quarantine_summary q ON q.url = p.url
    LEFT JOIN event_summary e ON e.product_id = p.product_id
    LEFT JOIN statement_competitor_data_warehouse.ref_shop_rules sr ON LOWER(sr.shop_name) = LOWER(p.shop_name)
    LEFT JOIN statement_competitor_data_warehouse.ref_shop_rules dr ON dr.shop_name = '*'
    LEFT JOIN statement_competitor_data_warehouse.gold_product_presence first_missed
        ON first_missed.product_id = p.product_id
       AND first_missed.scrape_seq = p.last_present_seq + 1
),
-- Sold signals as flags (none for quarantined products: no valid price to sell at)
classified AS (
    SELECT
        *,
        NOT is_quarantined AND scrapes_missed >= grace AS is_delisted,
        current_available IS FALSE AS is_marked_sold,
        COALESCE(current_price = 0 AND zero_price_means_sold, FALSE) AS is_zero_priced
    FROM combined
),
-- Overall sold flag and sold time
with_sold_at AS (
    SELECT
        *,
        is_delisted OR is_marked_sold OR is_zero_priced AS is_sold,
        -- Earliest of the active sold signals
        (SELECT MIN(t) FROM UNNEST([
            IF(is_delisted, first_missed_at, NULL),
            IF(is_marked_sold OR is_zero_priced, COALESCE(last_sold_signal_at, current_state_since), NULL)
        ]) AS t) AS sold_at
    FROM classified
)
SELECT
    product_id,
    url,
    shop_name,
    CASE
        WHEN is_quarantined THEN 'QUARANTINED'
        WHEN is_sold THEN 'SOLD'
        WHEN scrapes_missed > 0 THEN 'PENDING_DELIST'
        WHEN last_listed_price < first_listed_price THEN 'ACTIVE_DISCOUNTED'
        ELSE 'ACTIVE'
    END AS status,
    -- Site signals are more specific than disappearance, so they take precedence
    CASE
        WHEN is_marked_sold THEN 'MARKED_SOLD'
        WHEN is_zero_priced THEN 'ZERO_PRICE'
        WHEN is_delisted THEN 'DELISTED'
    END AS sold_reason,
    sold_at,
    IF(is_sold, last_listed_price, NULL) AS sold_price,
    -- Days from first sighting to sale (or to today if still listed)
    DATE_DIFF(DATE(COALESCE(sold_at, CURRENT_TIMESTAMP())), DATE(first_seen_at), DAY) AS days_on_market,
    current_price,
    current_available,
    first_listed_price,
    last_listed_price,
    markdown_count,
    IF(
        last_listed_price < first_listed_price,
        ROUND(SAFE_DIVIDE(first_listed_price - last_listed_price, first_listed_price) * 100, 1),
        0
    ) AS total_markdown_pct,
    first_seen_at,
    last_seen_at,
    scrapes_missed,
    grace AS delist_grace_scrapes,
    IF(is_quarantined, quarantine_reason, NULL) AS quarantine_reason
FROM with_sold_at;


-- -----------------------------------------------------------------------------
-- gold_products_flat: one row per product with all attributes, current price
-- and lifecycle status. Single data source for Google Data Studio.
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW statement_competitor_data_warehouse.gold_products_flat
OPTIONS(description="One row per competitor product: shop, category, maker, material, current price and lifecycle status. Data Studio source.")
AS
SELECT
    f.product_id,
    f.url,
    f.title,
    s.shop_name,
    s.region,
    s.city,
    s.latitude,
    s.longitude,
    c.category,
    m.makers,
    mat.primary_material,
    f.era,
    f.condition,
    f.dimensions,
    st.status,
    st.sold_reason,
    st.sold_at,
    st.sold_price,
    st.current_price,
    st.current_available,
    st.first_listed_price,
    st.last_listed_price,
    st.markdown_count,
    st.total_markdown_pct,
    st.days_on_market,
    st.first_seen_at,
    st.last_seen_at
FROM statement_competitor_data_warehouse.fact_product_scrape f
LEFT JOIN statement_competitor_data_warehouse.dim_shop s ON s.shop_id = f.shop_id
LEFT JOIN statement_competitor_data_warehouse.dim_category c ON c.category_id = f.category_id
LEFT JOIN statement_competitor_data_warehouse.dim_maker m ON m.maker_id = f.maker_id
LEFT JOIN statement_competitor_data_warehouse.dim_material mat ON mat.material_id = f.material_id
LEFT JOIN statement_competitor_data_warehouse.gold_product_status st ON st.product_id = f.product_id;




/*
===============================================================================
Tests (run manually)
===============================================================================
-- All four lifecycle views exist (expect table_type = VIEW).
SELECT table_name, table_type
FROM `statement_competitor_data_warehouse.INFORMATION_SCHEMA.TABLES`
WHERE table_name IN ('gold_product_presence', 'gold_product_events', 'gold_product_status', 'gold_products_flat')
ORDER BY table_name;

-- One status row per product (expect no rows).
SELECT product_id, COUNT(*) AS n
FROM statement_competitor_data_warehouse.gold_product_status
GROUP BY product_id
HAVING COUNT(*) > 1;

-- Status overview per shop.
SELECT shop_name, status, COUNT(*) AS products, ROUND(AVG(sold_price), 2) AS avg_sold_price
FROM statement_competitor_data_warehouse.gold_product_status
GROUP BY shop_name, status
ORDER BY shop_name, status;

-- Quarantined products and why (scraped, never passed validation).
SELECT shop_name, quarantine_reason, COUNT(*) AS products
FROM statement_competitor_data_warehouse.gold_product_status
WHERE status = 'QUARANTINED'
GROUP BY shop_name, quarantine_reason
ORDER BY products DESC;

-- Event counts per type and shop. Many SCRAPE_GAP events suggest scraper issues.
SELECT shop_name, event_type, COUNT(*) AS events
FROM statement_competitor_data_warehouse.gold_product_events
GROUP BY shop_name, event_type
ORDER BY shop_name, event_type;

-- Sold products with days on market and discount before sale.
SELECT shop_name, url, sold_reason, sold_at, sold_price, first_listed_price, total_markdown_pct, days_on_market
FROM statement_competitor_data_warehouse.gold_product_status
WHERE status = 'SOLD'
ORDER BY sold_at DESC;

-- Full event history of one product.
SELECT event_type, event_time, price, previous_price, price_change_pct, detail
FROM statement_competitor_data_warehouse.gold_product_events
WHERE url = 'https://...'
ORDER BY event_time;

-- Flat view row count matches the fact table.
SELECT
    (SELECT COUNT(*) FROM statement_competitor_data_warehouse.gold_products_flat) AS flat_rows,
    (SELECT COUNT(*) FROM statement_competitor_data_warehouse.fact_product_scrape) AS fact_rows;
*/
