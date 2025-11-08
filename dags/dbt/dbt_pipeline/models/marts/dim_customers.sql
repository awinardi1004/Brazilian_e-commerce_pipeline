{{
    config(
        materialized= 'table',
        post_hook="ALTER TABLE {{ this }} ADD PRIMARY KEY (customer_id)"
    )
}}

WITH latest_address AS (
    SELECT
        "customer_unique_id" as customer_id,
        "customer_zip_code_prefix" as zip_code,
        "customer_city" as city,
        "customer_state" as state,
        ROW_NUMBER() OVER (
            PARTITION BY "customer_unique_id"
            ORDER BY MAX("order_purchase_timestamp") DESC
        ) AS rn
    FROM {{ source('staging', 'STG_CUSTOMERS')}} c
    JOIN {{ source('staging', 'STG_ORDERS')}} o ON c."customer_id" = o."customer_id"
    GROUP BY "customer_unique_id", "customer_zip_code_prefix", "customer_city", "customer_state"
)
SELECT
    customer_id,
    city,
    state,
    zip_code
FROM latest_address
WHERE rn = 1
