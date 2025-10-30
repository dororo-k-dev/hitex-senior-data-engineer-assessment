-- Complete Business Data Model for HITEx

-- Core Dimension Tables
CREATE OR REPLACE TABLE `hitex_core.dim_product` (
    product_id STRING NOT NULL,
    sku STRING,
    product_name STRING,
    category STRING,
    subcategory STRING,
    brand STRING,
    cost_price FLOAT64,
    retail_price FLOAT64,
    created_date DATE,
    is_active BOOL
);

CREATE OR REPLACE TABLE `hitex_core.dim_customer` (
    customer_id STRING NOT NULL,
    customer_source_id STRING,
    channel_id STRING,
    first_name STRING,
    last_name STRING,
    email STRING,
    phone STRING,
    address STRING,
    city STRING,
    state STRING,
    country STRING,
    created_date DATE
);

CREATE OR REPLACE TABLE `hitex_core.dim_channel` (
    channel_id STRING NOT NULL,
    channel_name STRING,
    channel_type STRING,
    description STRING
);

CREATE OR REPLACE TABLE `hitex_core.dim_location` (
    location_id STRING NOT NULL,
    location_name STRING,
    location_type STRING,
    address STRING,
    city STRING,
    state STRING,
    country STRING,
    is_active BOOL
);

-- Fact Tables
CREATE OR REPLACE TABLE `hitex_core.fact_sales` (
    sales_id STRING NOT NULL,
    date_key INT64,
    product_id STRING,
    customer_id STRING,
    channel_id STRING,
    location_id STRING,
    order_id STRING,
    quantity_sold INT64,
    unit_price FLOAT64,
    total_amount FLOAT64,
    discount_amount FLOAT64,
    net_amount FLOAT64,
    cost_amount FLOAT64,
    profit_amount FLOAT64,
    transaction_time TIMESTAMP
);

CREATE OR REPLACE TABLE `hitex_core.fact_inventory` (
    inventory_id STRING NOT NULL,
    date_key INT64,
    product_id STRING,
    location_id STRING,
    opening_stock INT64,
    closing_stock INT64,
    quantity_received INT64,
    quantity_sold INT64,
    quantity_adjusted INT64,
    stock_value FLOAT64
);

-- Business Mart Views
CREATE OR REPLACE VIEW `hitex_mart.sales_summary` AS
SELECT 
    c.channel_name,
    p.product_name,
    p.category,
    DATE(t.transaction_time) as sales_date,
    SUM(f.quantity_sold) as total_quantity,
    SUM(f.net_amount) as total_revenue,
    SUM(f.profit_amount) as total_profit
FROM `hitex_core.fact_sales` f
JOIN `hitex_core.dim_channel` c ON f.channel_id = c.channel_id
JOIN `hitex_core.dim_product` p ON f.product_id = p.product_id
JOIN `hitex_core.dim_date` t ON f.date_key = t.date_key
GROUP BY c.channel_name, p.product_name, p.category, sales_date;