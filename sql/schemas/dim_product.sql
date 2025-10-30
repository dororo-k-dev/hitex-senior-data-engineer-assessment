-- HITEx Product Dimension Table (SCD-2 Implementation)
CREATE TABLE IF NOT EXISTS `${project_id}.curated.dim_product` (
    product_key STRING NOT NULL,
    product_id STRING NOT NULL,
    product_name STRING NOT NULL,
    category STRING,
    brand STRING,
    unit_cost NUMERIC(10,2),
    
    -- SCD-2 Fields
    effective_date DATETIME NOT NULL,
    end_date DATETIME NOT NULL,
    is_current BOOLEAN NOT NULL DEFAULT TRUE,
    record_hash STRING NOT NULL,
    
    -- Audit Fields
    created_date DATETIME DEFAULT CURRENT_DATETIME(),
    updated_date DATETIME DEFAULT CURRENT_DATETIME(),
    batch_id STRING,
    
    -- Constraints
    PRIMARY KEY (product_key) NOT ENFORCED
)
PARTITION BY DATE(effective_date)
CLUSTER BY product_id, is_current
OPTIONS (
    description = "Product dimension with SCD-2 for tracking historical changes",
    labels = [("env", "production"), ("team", "data-engineering")]
);