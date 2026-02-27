{{ config(materialized='table') }}

SELECT 
    status,
    COUNT(order_id) AS total_orders,
    ROUND(SUM(sale_price), 2) AS total_revenue
FROM {{ source('my_raw_data', 'LARGE_SAMPLE_ORDERS') }}
GROUP BY status
ORDER BY total_revenue DESC