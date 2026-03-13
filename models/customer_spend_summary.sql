{{ config(materialized='table') }}

WITH customer_orders AS (
    SELECT 
        user_id,
        COUNT(DISTINCT order_id) AS total_orders,
        SUM(sale_price) AS total_spend,
        AVG(sale_price) AS average_order_value,
        MAX(sale_price) AS max_order_value,
        
        MIN(sale_price) AS min_order_value
    FROM {{ source('my_raw_data', 'LARGE_SAMPLE_ORDERS') }}
    WHERE status NOT IN ('Cancelled', 'Returned')
    GROUP BY 1
)

SELECT
    user_id,
    total_orders,
    ROUND(total_spend, 2) AS total_spend,
    ROUND(average_order_value, 2) AS average_order_value,
    ROUND(max_order_value, 2) AS max_order_value,
    ROUND(min_order_value, 2) AS min_order_value,
    CASE 
        WHEN total_spend > 1000 THEN 'High Value'
        WHEN total_spend > 500 THEN 'Medium Value'
        ELSE 'Low Value'
    END AS customer_segment
FROM customer_orders
ORDER BY total_spend DESC
