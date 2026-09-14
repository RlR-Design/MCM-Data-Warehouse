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

## **6\. Technical Challenges & Data Complexities**

Extracting and modeling data from this specific e-commerce domain introduces significant technical hurdles that the ETL pipeline must address:

### **6.1. Web Scraping & Access Challenges**

* **Anti-Bot Evasion & Rate Limiting:** Modern e-commerce platforms (Shopify, Wix) employ stringent bot detection (e.g., Cloudflare, TLS fingerprinting, IP reputation scoring). The scraping architecture must manage access dynamically using proxy rotation, intelligent request throttling (polite crawling), and realistic user-agent spoofing to prevent IP bans and HTTP 403 blocks.  
* **Dynamic DOM & Client-Side Rendering:** Many target storefronts utilise JavaScript-heavy rendering where crucial pricing or inventory data is injected client-side post-load. The scrapers must utilise headless browsers (e.g., Playwright or Selenium) rather than basic HTTP requests to ensure accurate capture of fully rendered DOM elements.  
* **UI Volatility:** Subtle changes to a competitor's website CSS classes or DOM structure can silently break extraction logic. The pipeline requires resilient XPath/CSS selectors and automated monitoring to alert the engineer when data volumes drop due to structural changes.

  ### **6.2. Data Extraction & Transformation Challenges**

* **Unstructured Descriptive Text & NLP:** Listings lack uniform form fields. Parsing logic requires advanced regex and semantic text extraction to isolate materials, dimensions, and condition tags buried within free-text descriptions.  
* **Inconsistent Taxonomy & Terminology:** Competitors classify identical styles or wood types using disparate nomenclature (e.g., mixing "Danish style", "Teak finish", and solid "Kiaat"). Here a LLM API will be used to account for this complexity and extract the necessary information. This will allow for more reliable handling of the data.&nbsp;&nbsp;&nbsp;  
* **Missing Attribution Labels:** Premium pieces often lack explicit designer tags in listing titles, necessitating keyword matching and dictionary lookups against known high-value makers to accurately attribute value.

  ### 

  ### **6.3. Data Modeling & ETL Challenges**

* **Data Persistence & Price Drift (Slowly Changing Dimensions):** E-commerce items are frequently removed, discounted, or marked "sold" without historic price logs. The architecture requires a robust snapshot mechanism (Type 2 SCD logic) in the `dim_price_history` table to accurately capture price adjustments and active status over time.  
* **Handling Malformed Data (Normalisation at Capture):** Raw e-commerce data is notoriously messy. Prices may contain mixed currency symbols or string text (e.g., "POA"). The Silver layer must strictly enforce data typing and reject/quarantine rows that do not fit the schema to maintain downstream analytical integrity.

## **7\. Data Architecture & Medallion Pipeline**

The architecture follows a strict three-tier Medallion model inside **Google Cloud BigQuery**:

### **Bronze Layer (Landing / Raw Data)**

* **Definition & Objective:** Stores raw, uncompressed scraped data exactly as received from source platforms to ensure complete traceability and debugging.  
* **Load Method:** Full load (truncate and insert) or batch processing via Cloud Functions.

### **Silver Layer (Staging, Cleaning & Validation)**

* **Definition & Objective:** An intermediate cleaning layer that normalises data, standardises terminology, and validates schema integrity.  
* **Process Flow:** Stored procedures validate mandatory fields (`url NOT NULL` and `price IS PARSEABLE`). Valid records populate staging views (`stg_clean_products`), while malformed payloads are routed to a `quarantine_bad_records` table with automated Slack alerts (`#dwh-alerts`).  
* **Transformations:** Data cleansing, standardisation, surrogate key generation, normalisation, derived column creation, and data enrichment.

### **Gold Layer (Analytics & Business-Ready)**

* **Definition & Objective:** Business-ready data modelled into analytical views and star schemas optimised for reporting.  
* **Transformations:** Data integration, aggregations, business rule application, and dimensional modeling.

## **8\. Entity Relationship Model & Schema Design (3NF / Star Schema)**

The analytical model utilises a Star Schema centered around the core fact table:

* **`fact_product_scrape` (Fact Table):**  
  * Primary Key: `product_id`  
  * Foreign Keys: `shop_id`, `category_id`, `maker_id`, `material_id`  
  * Attributes: `url` (UK), `title`, `description` (TEXT), `era`, `condition`, `dimension`  
* **`dim_shop` (Dimension):** `shop_id` (PK), `shop_name`, `region` (e.g., Gauteng vs. Western Cape), `city`, `latitude`, `longitude`  
* **`dim_material` (Dimension):** `material_id` (PK), `material_name` (e.g., Kiaat, Teak, Imbuia)  
* **`dim_maker` (Dimension):** `maker_id` (PK), `maker_name`  
* **`dim_category` (Dimension):** `category_id` (PK), `category_name` (e.g., Sideboard, Seating, Dining Table)  
* **`dim_price_history` (Dimension / History):** `price_log_id` (PK), `product_id` (FK), `scrape_timestamp`, `price_zar` (DECIMAL), `available` (BOOLEAN)

## **9\. Key Analytical Metrics**

* **Valuation Benchmarks:** Median/interquartal median and mean listing prices segmented by product category, maker and material.  
* **Attribution Premiums:** Designer attribution premium ratio comparing branded pieces against unbranded equivalents.  
* **Inventory Velocity:** Average days on market grouped by price point, category, and regional hub (Gauteng vs. Western Cape).  
* **Market Activity:** Listing active rates and historical price variance per dealer.

## **10\. Scope & Sample Strategy**

The initial dataset captures active listings from primary South African online vintage stockists and regional auction aggregators. Focusing the sample on high-demand categories, specifically **sideboards, seating, and dining tables**, ensures statistically significant volume while controlling for noise from one-off architectural antiques or non-standard decorative items.

