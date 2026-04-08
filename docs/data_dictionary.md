# Data Dictionary — Sales Data Warehouse

## Overview
Star Schema with 4 dimension tables and 1 fact table, built on Databricks + Delta Lake.

---

## GOLD Layer — Star Schema

### fact_sales
| Column | Type | Description |
|--------|------|-------------|
| customer_sk | LONG | FK → dim_customer.customer_sk |
| product_sk | LONG | FK → dim_product.product_sk |
| order_date_sk | INT | FK → dim_date.date_key (order date) |
| ship_date_sk | INT | FK → dim_date.date_key (ship date) |
| geography_sk | LONG | FK → dim_geography.geography_sk |
| order_id | STRING | Order identifier (degenerate dimension) |
| order_line_id | STRING | Unique line item key (grain) |
| payment_method | STRING | Credit Card / Debit Card / PayPal |
| order_channel | STRING | Web / Mobile |
| order_status | STRING | Processing / Shipped / Delivered |
| quantity | INT | Units ordered |
| unit_price | DECIMAL(10,2) | Selling price per unit |
| unit_cost | DECIMAL(10,2) | Cost price per unit |
| discount_pct | DECIMAL(5,4) | Discount percentage (0–1) |
| discount_amount | DECIMAL(10,2) | Total discount in currency |
| gross_revenue | DECIMAL(10,2) | quantity × unit_price |
| net_revenue | DECIMAL(10,2) | gross_revenue − discount_amount |
| cogs | DECIMAL(10,2) | Cost of Goods Sold (quantity × unit_cost) |
| gross_profit | DECIMAL(10,2) | net_revenue − cogs |
| gross_profit_pct | DECIMAL(5,2) | gross_profit / net_revenue × 100 |
| days_to_ship | INT | ship_date − order_date |
| order_date | DATE | Partition column |
| source_system | STRING | Originating system |
| batch_id | STRING | ETL batch run ID |
| created_at | TIMESTAMP | Row creation time |
| updated_at | TIMESTAMP | Last update time (SCD1) |

---

### dim_customer (SCD Type 1)
| Column | Type | Description |
|--------|------|-------------|
| customer_sk | LONG | Surrogate key (hash-based) |
| customer_bk | STRING | Business key from CRM |
| first_name | STRING | First name |
| last_name | STRING | Last name |
| full_name | STRING | Concatenated full name |
| email | STRING | Email address (lower) |
| phone | STRING | Phone number (digits only) |
| city | STRING | City |
| state | STRING | State code (upper) |
| country | STRING | Country code (upper) |
| zip_code | STRING | Postal code |
| customer_segment | STRING | STANDARD / PREMIUM / VIP |
| registration_date | DATE | Account creation date |
| created_at | TIMESTAMP | Row creation time |
| updated_at | TIMESTAMP | Last SCD1 overwrite time |

---

### dim_product (SCD Type 1)
| Column | Type | Description |
|--------|------|-------------|
| product_sk | LONG | Surrogate key |
| product_bk | STRING | Business key from ERP |
| product_name | STRING | Product name |
| category | STRING | Top-level category |
| sub_category | STRING | Sub-category |
| brand | STRING | Brand name |
| sku | STRING | Stock keeping unit |
| supplier_id | STRING | Supplier reference |
| unit_cost | DECIMAL(10,2) | Cost price |
| unit_price | DECIMAL(10,2) | Retail price |
| gross_margin_pct | DECIMAL(5,2) | (price−cost)/price × 100 |
| weight_kg | DECIMAL(8,3) | Product weight |
| is_active | BOOLEAN | Active in catalog |
| created_date | DATE | Product launch date |
| created_at | TIMESTAMP | Row creation time |
| updated_at | TIMESTAMP | Last SCD1 overwrite time |

---

### dim_date (Static)
| Column | Type | Description |
|--------|------|-------------|
| date_key | INT | YYYYMMDD integer key |
| full_date | DATE | Calendar date |
| year | INT | Calendar year |
| quarter | INT | 1–4 |
| quarter_name | STRING | Q1–Q4 |
| month | INT | 1–12 |
| month_name | STRING | January–December |
| month_abbrev | STRING | Jan–Dec |
| week_of_year | INT | ISO week number |
| day_of_month | INT | 1–31 |
| day_of_week | INT | 1=Sun, 7=Sat |
| day_name | STRING | Monday–Sunday |
| is_weekend | BOOLEAN | Saturday or Sunday |
| year_month | STRING | yyyy-MM |
| year_quarter | STRING | yyyy-Q1 |
| fiscal_year | INT | Fiscal year (Jul start) |
| fiscal_quarter | STRING | FQ1–FQ4 |

---

### dim_geography (SCD Type 1)
| Column | Type | Description |
|--------|------|-------------|
| geography_sk | LONG | Surrogate key |
| geography_bk | STRING | country_state_city composite |
| city | STRING | City name |
| state | STRING | State/Province code |
| country | STRING | Country code |
| zip_code | STRING | Postal/ZIP code |
| created_at | TIMESTAMP | Row creation time |
| updated_at | TIMESTAMP | Last SCD1 overwrite time |

---

## SILVER Layer

### silver.customers
Cleansed CRM customers: type-cast, deduplicated, standardised casing.

### silver.products
Cleansed ERP products: numeric types enforced, invalid pricing removed, gross_margin_pct derived.

### silver.orders
Cleansed orders: composite key deduplication, financial metrics derived (gross_revenue, net_revenue, discount_amount, days_to_ship).

---

## BRONZE Layer

### bronze.crm_customers
Raw CRM data — all columns as strings + audit columns (ingested_at, source_system, batch_id).

### bronze.erp_products
Raw ERP data — all columns as strings + audit columns.

### bronze.ecom_orders
Raw e-commerce order data — all columns as strings + audit columns. Partitioned by order_date.

---

## SCD Type 1 — Behaviour Summary

| Event | Action |
|-------|--------|
| New record arrives | INSERT with new surrogate key |
| Existing record unchanged | No action (no match on changed cols) |
| Existing record changed | UPDATE all non-key attributes; updated_at = now() |
| Record deleted from source | No action (not tracked) |
