-- Business mart layer - Sales summary
CREATE OR REPLACE TABLE `hitex_mart.sales_summary` AS
SELECT
    channel_name,
    product_name,
    category,
    DATE(transaction_time) as sales_date,
    SUM(quantity_sold) as total_quantity,
    SUM(net_amount) as total_revenue,
    SUM(profit_amount) as total_profit,
    COUNT(DISTINCT order_id) as order_count
FROM `hitex_core.fact_sales` fs
JOIN `hitex_core.dim_channel` dc ON fs.channel_id = dc.channel_id
JOIN `hitex_core.dim_product` dp ON fs.product_id = dp.product_id
GROUP BY channel_name, product_name, category, sales_date;