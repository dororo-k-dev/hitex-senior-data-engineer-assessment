-- HITEx Sales Fact Table
CREATE TABLE IF NOT EXISTS `${project_id}.curated.fct_sales` (
    sales_key STRING NOT NULL,
    order_id STRING NOT NULL,
    product_id STRING NOT NULL,
    customer_id STRING NOT NULL,
    
    -- Measures
    quantity INTEGER NOT NULL,
    unit_price NUMERIC(10,2) NOT NULL,
    total_amount NUMERIC(12,2) NOT NULL,
    
    -- Dimensions
    channel STRING NOT NULL,
    region STRING,
    order_date DATETIME NOT NULL,
    
    -- Audit Fields
    load_date DATETIME DEFAULT CURRENT_DATETIME(),
    batch_id STRING NOT NULL,
    source_system STRING DEFAULT 'CSV_IMPORT',
    
    -- Data Quality Fields
    quality_score NUMERIC(5,2),
    validation_status STRING DEFAULT 'PASSED',
    
    -- Constraints
    PRIMARY KEY (sales_key) NOT ENFORCED,
    
    -- Check Constraints
    CHECK (quantity > 0),
    CHECK (unit_price >= 0),
    CHECK (total_amount >= 0)
)
PARTITION BY DATE(order_date)
CLUSTER BY channel, product_id, customer_id
OPTIONS (
    description = "Sales fact table with comprehensive audit trail",
    labels = [("env", "production"), ("team", "data-engineering")]
);