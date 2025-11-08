SELECT
    order_id,
    product_id,
    COUNT(*) AS duplicate_count
FROM {{ ref('fact_order_items') }}
GROUP BY order_id, product_id
HAVING COUNT(*) > 1
