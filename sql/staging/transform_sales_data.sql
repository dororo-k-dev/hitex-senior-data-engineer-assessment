-- Transform raw sales data to staging
CREATE OR REPLACE TABLE `hitex_staging.sales_clean` AS
SELECT
    order_id,
    product_id,
    customer_id,
    channel,
    quantity,
    unit_price,
    total_amount,
    order_date,
    processed_at,
    batch_id
FROM `hitex_raw.sales_data`
WHERE order_id IS NOT NULL
  AND product_id IS NOT NULL
  AND quantity > 0
  AND unit_price > 0;