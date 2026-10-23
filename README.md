# MCM-Data-Warehouse-Analytics
Building a modern data warehouse in BigQuery for a Mid-century Modern furniture company

## **1\. Executive Summary**

This project establishes an empirical pricing baseline and automated data pipeline for the South African mid-century modern (MCM) and vintage furniture market. Independent dealers and restorers, exemplified by **omitted**, frequently operate in a fragmented landscape relying on intuition or ad-hoc competitor checks. This often leads to mispriced inventory due to unstandardised listings, inconsistent timber classifications (e.g., Kiaat, Teak, Imbuia), and obscured designer attributions.

To resolve this, the project implements an end-to-end data warehouse utilising a **Medallion Architecture (Bronze, Silver, Gold)** hosted on **Google Cloud BigQuery**. Custom Python web scrapers and cloud functions extract weekly/monthly listing feeds across key South African boutique storefronts and platforms (Shopify, WooCommerce, Wix). Data flows through robust staging validation, quarantine logging, and star-schema dimensional modelling. The final strategic output powers an interactive **Google Data Studio** KPI dashboard, delivering automated catalog tracking, regional demand analytics (Gauteng vs. Western Cape), turnover velocity metrics, and data-driven retail pricing intelligence.&nbsp;

## **2\. Problem Domain**

The South African vintage, retro, and mid-century modern furniture market operates through a niche network of boutique restorers, independent online stores, and secondary marketplace platforms (including stockists such as *content was deleted to preserve anonymity* and *omitted*). Unlike mass-market retail, vintage asset valuation depends heavily on scarce qualitative attributes: raw wood species, historical design attribution, physical restoration condition, era provenance, and regional demand dynamics.

## **3\. Problem Statement**

Independent furniture dealers in South Africa frequently set retail prices based on personal intuition or isolated competitor checks. Because item descriptions, wood species (such as Kiaat, Imbuia, and Teak), and designer attributions are unstandardised across platforms, businesses risk undervaluing rare inventory or locking up capital in overpriced, slow-moving stock. Without a centralised historical data repository, tracking market price drift and true inventory velocity across competitors remains unfeasible.

## **4\. Business Objectives**

* **Establish an Empirical Pricing Baseline:** Quantify median/quartile median and mean market values for MCM furniture across major South African digital sales channels.  
* **Isolate Value Drivers:** Identify specific item characteristics, timber types, and designer tags that drive measurable price premiums.  
* **Benchmark Inventory Velocity:** Calculate average days on market and turnover velocity to benchmark performance against regional competitors.  
* **Build a Scalable Data Pipeline:** Design a normalised database schema and automated ETL workflow capable of transitioning from static snapshots to scheduled monthly/weekly pipelines.  
* **Deliver Actionable Intelligence:** Deploy an intuitive Google Data Studio executive dashboard tracking key pricing matrices, inventory health, and regional demand concentrations.

## **5\. Stakeholders & Target Audience**

* **Vintage Furniture Business Owners & Restorers (omitted):** Require data-driven pricing models to optimise acquisition budgets, set competitive listing prices, and evaluate stock performance.  
* **E-commerce & Brand Strategists:** Require granular market intelligence to optimise product cataloguing, search keyword strategies, and collection structures across digital storefronts.


## **6\. How to Run**

All objects live in the BigQuery dataset `statement_competitor_data_warehouse`. The full operation and maintenance guide is in [03_Documentation/Manual.md](03_Documentation/Manual.md). Each script starts with a header (purpose, when to run, requirements, warnings) and ends with commented-out tests.

### Setup (once, in order)

| Step | Script | Creates |
|---|---|---|
| 1 | `01_DDL/01_create_dataset.sql` | Dataset |
| 2 | `01_DDL/02_create_bronze_tables.sql` | Bronze tables |
| 3 | `01_DDL/03_create_silver_tables.sql` | Silver tables, star schema, price history |
| 4 | `01_DDL/04_create_reference_tables.sql` | `ref_shop_locations`, `ref_shop_rules` |
| 5 | `01_DDL/05_create_gold_views.sql` | Gold star schema views |
| 6 | `01_DDL/06_create_gold_lifecycle_views.sql` | Gold lifecycle and reporting views |
| 7 | `01_DDL/07_create_gold_market_views.sql` | Gold market insight views |
| 8 | `01_DDL/08_create_monitoring_views.sql` | Scrape monitoring views |
| 9 | `02_Stored_Procedures/01` to `09` | Stored procedures |

DDL scripts are safe to re-run: they never drop or overwrite data. Re-deploy a procedure script after changing it.

### Pipeline (per scraped CSV)

```sql
CALL statement_competitor_data_warehouse.sp_run_pipeline('gs://<bucket>/<file>.csv');
```

`sp_run_pipeline` runs these steps in order and stops at the first failure:

| Step | Procedure | Does |
|---|---|---|
| 1 | `sp_load_bronze_products` | Appends the CSV to Bronze as a new batch |
| 2 | `sp_validate_bronze_products` | Quarantines rows without a URL or parseable price |
| 3 | `sp_check_scrape_health` | Flags failed or partial shop scrapes (never stops the run) |
| 4 | `sp_load_silver_products` | Cleans the batch and merges new or changed products into Silver |
| 5 | `sp_validate_silver_products` | Quarantines rows without a shop name or valid price |
| 6 | `sp_normalise_silver_products` | Rebuilds dimensions and the fact table |
| 7 | `sp_load_dim_price_history` | Records price and availability changes (SCD Type 2) |
| 8 | `sp_reconciled_silver_products` | Runs integrity checks and logs any issues |

Gold objects are views and need no load step.

### Gold views for reporting

| View | Grain | Answers |
|---|---|---|
| `gold_products_flat` | product | All attributes, current price and status |
| `gold_sales` | sale | Every sale nationally |
| `gold_price_baseline` | segment | Price quartiles per category, material, maker, era, condition, region, shop |
| `gold_value_drivers` | category x attribute | Price premium of materials, makers, eras and conditions |
| `gold_market_monthly` | month x region x category | New listings, sales and prices over time |
| `gold_regional_demand` | region | Demand, stock and velocity per province |
| `gold_shop_scorecard` | shop | Competitor benchmark |

Medians are pre-computed per row: filter them in Data Studio, do not sum or average them.

### Reference data

Maintained by hand, no redeploy needed:

- `ref_shop_locations`: verified shop coordinates.
- `ref_shop_rules`: how each shop signals a sale (zero price, delisting grace period) and its scrape health thresholds. The `'*'` row is the default.

### Scrape health

`bronze_scrape_health` holds one row per shop per batch: `HEALTHY`, `WARNING` or `FAILED`, with the reasons. Failed scrapes are still loaded, but the Gold views ignore them when deciding whether a product was delisted.

Monitoring views:

- `mon_scrape_runs`: one row per run with overall status, shops per status, product totals and duration.
- `mon_scrape_shop_runs`: one row per shop per run with the scraper's report next to the warehouse checks.

### Scrape run summary file

Upload next to the products CSV, in the same folder, named `<name>_run_summary.csv` (e.g. `MCM_All_products.csv` and `MCM_All_products_run_summary.csv`). It is optional; without it, only the warehouse-side checks run. `04_Scraper/scrape_run_summary.py` writes it.

| Column | Example | Meaning |
|---|---|---|
| `run_id` | `2b47d3b0-...` | Same for every shop in one run |
| `shop_name` | `Retro MCM` | Exactly as in the products CSV |
| `started_at` / `finished_at` | `2026-10-22 08:00:00` | UTC |
| `status` | `SUCCESS`, `PARTIAL`, `FAILED` | Scraper's own verdict |
| `pages_scraped` | `13` | Listing pages visited |
| `products_found` | `312` | Products extracted |
| `products_expected` | `312` | Count shown by the site, blank if not shown |
| `http_errors` / `retries` | `0` | Request failures and retries |
| `error_message` | `TimeoutError: ...` | Blank on success |
| `scraper_version` | `1.0.0` | Scraper release |
