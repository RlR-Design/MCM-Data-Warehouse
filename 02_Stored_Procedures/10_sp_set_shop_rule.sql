/*
===============================================================================
Script   : 10_sp_set_shop_rule.sql
Layer    : Reference
Purpose  : Creates or updates one shop's row in ref_shop_rules: lifecycle rules
           and scrape health thresholds. Use '*' to change the default rule.
When     : Deploy at setup. CALL whenever a shop behaves differently from the
           default. Not part of sp_run_pipeline.
Requires : 01_DDL/04_create_reference_tables.sql
Params   : p_shop_name              Shop name as scraped (case-insensitive), or '*'
           p_zero_price_means_sold  TRUE if R0.00 means sold
           p_delist_grace_scrapes   Scrapes missing before delisted (1 or more)
           p_min_count_ratio        FAILED below this share of the baseline (0-1)
           p_max_quarantine_rate    FAILED above this share quarantined (0-1)
           p_max_duplicate_rate     WARNING above this share of repeated URLs (0-1)
           p_max_field_empty_rate   WARNING above this share of empty fields (0-1)
           p_min_expected_ratio     FAILED below this share of the site's count (0-1)
           p_notes                  Why the shop has its own rule
           NULL keeps the current value; on a new row it falls back to '*'.
Warning  : Takes effect from the next pipeline run. Re-run sp_check_scrape_health
           for an earlier batch to re-check it; Gold views update immediately.
===============================================================================
*/

CREATE OR REPLACE PROCEDURE statement_competitor_data_warehouse.sp_set_shop_rule (
    IN p_shop_name STRING,
    IN p_zero_price_means_sold BOOL,
    IN p_delist_grace_scrapes INT64,
    IN p_min_count_ratio FLOAT64,
    IN p_max_quarantine_rate FLOAT64,
    IN p_max_duplicate_rate FLOAT64,
    IN p_max_field_empty_rate FLOAT64,
    IN p_min_expected_ratio FLOAT64,
    IN p_notes STRING
)
BEGIN
    -- Validate the inputs.
    IF p_shop_name IS NULL OR TRIM(p_shop_name) = '' THEN
        RAISE USING MESSAGE = "sp_set_shop_rule: p_shop_name is required (use '*' for the default rule).";
    END IF;

    IF p_delist_grace_scrapes IS NOT NULL AND p_delist_grace_scrapes < 1 THEN
        RAISE USING MESSAGE = 'sp_set_shop_rule: p_delist_grace_scrapes must be 1 or more.';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM UNNEST([p_min_count_ratio, p_max_quarantine_rate, p_max_duplicate_rate,
                     p_max_field_empty_rate, p_min_expected_ratio]) AS ratio
        WHERE ratio < 0 OR ratio > 1
    ) THEN
        RAISE USING MESSAGE = 'sp_set_shop_rule: ratios and rates must be between 0 and 1 (e.g. 0.3 = 30%).';
    END IF;

    -- Update the shop's row, or add it. NULL parameters keep the current value.
    MERGE statement_competitor_data_warehouse.ref_shop_rules AS target
    USING (SELECT TRIM(p_shop_name) AS shop_name) AS source
    ON LOWER(target.shop_name) = LOWER(source.shop_name)
    WHEN MATCHED THEN
        UPDATE SET
            zero_price_means_sold = COALESCE(p_zero_price_means_sold, target.zero_price_means_sold),
            delist_grace_scrapes = COALESCE(p_delist_grace_scrapes, target.delist_grace_scrapes),
            min_count_ratio = COALESCE(p_min_count_ratio, target.min_count_ratio),
            max_quarantine_rate = COALESCE(p_max_quarantine_rate, target.max_quarantine_rate),
            max_duplicate_rate = COALESCE(p_max_duplicate_rate, target.max_duplicate_rate),
            max_field_empty_rate = COALESCE(p_max_field_empty_rate, target.max_field_empty_rate),
            min_expected_ratio = COALESCE(p_min_expected_ratio, target.min_expected_ratio),
            notes = COALESCE(p_notes, target.notes),
            updated_at = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED THEN
        INSERT (shop_name, zero_price_means_sold, delist_grace_scrapes, min_count_ratio,
                max_quarantine_rate, max_duplicate_rate, max_field_empty_rate,
                min_expected_ratio, notes, updated_at)
        VALUES (source.shop_name, p_zero_price_means_sold, p_delist_grace_scrapes, p_min_count_ratio,
                p_max_quarantine_rate, p_max_duplicate_rate, p_max_field_empty_rate,
                p_min_expected_ratio, p_notes, CURRENT_TIMESTAMP());
END;


/*
===============================================================================
Tests (run manually)
===============================================================================
-- Allow up to 30% quarantined rows for one shop; everything else stays as is.
CALL statement_competitor_data_warehouse.sp_set_shop_rule(
    'Statement Collectables', NULL, NULL, NULL, 0.3, NULL, NULL, NULL,
    'Removes the price of sold items; up to 30% rows without a price is normal');

-- Change the default delisting grace period for all shops.
CALL statement_competitor_data_warehouse.sp_set_shop_rule('*', NULL, 3, NULL, NULL, NULL, NULL, NULL, NULL);

-- Fails: rates are fractions, not percentages.
CALL statement_competitor_data_warehouse.sp_set_shop_rule('shop', NULL, NULL, NULL, 30, NULL, NULL, NULL, NULL);

-- Rules in effect per shop (shop value, else default).
SELECT
    s.shop_name,
    COALESCE(r.zero_price_means_sold, d.zero_price_means_sold) AS zero_price_means_sold,
    COALESCE(r.delist_grace_scrapes, d.delist_grace_scrapes) AS delist_grace_scrapes,
    COALESCE(r.min_count_ratio, d.min_count_ratio) AS min_count_ratio,
    COALESCE(r.max_quarantine_rate, d.max_quarantine_rate) AS max_quarantine_rate,
    COALESCE(r.max_duplicate_rate, d.max_duplicate_rate) AS max_duplicate_rate,
    COALESCE(r.max_field_empty_rate, d.max_field_empty_rate) AS max_field_empty_rate,
    COALESCE(r.min_expected_ratio, d.min_expected_ratio) AS min_expected_ratio,
    r.notes
FROM statement_competitor_data_warehouse.dim_shop s
LEFT JOIN statement_competitor_data_warehouse.ref_shop_rules r ON LOWER(r.shop_name) = LOWER(s.shop_name)
CROSS JOIN (SELECT * FROM statement_competitor_data_warehouse.ref_shop_rules WHERE shop_name = '*') d
ORDER BY s.shop_name;

-- Clear a shop value so it falls back to the default again.
UPDATE statement_competitor_data_warehouse.ref_shop_rules
SET max_quarantine_rate = NULL, updated_at = CURRENT_TIMESTAMP()
WHERE LOWER(shop_name) = LOWER('<shop>');
*/
