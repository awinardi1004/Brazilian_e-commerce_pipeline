SELECT
    *
FROM {{ ref('fact_orders') }}
WHERE
    (
        approved_timestamp_id IS NOT NULL
        AND purchase_timestamp_id > approved_timestamp_id
    )
    OR (
        delivered_carrier_timestamp_id IS NOT NULL
        AND approved_timestamp_id IS NOT NULL
        AND approved_timestamp_id > delivered_carrier_timestamp_id
    )
    OR (
        delivered_customer_timestamp_id IS NOT NULL
        AND delivered_carrier_timestamp_id IS NOT NULL
        AND delivered_carrier_timestamp_id > delivered_customer_timestamp_id
    )
    OR (
        estimated_delivery_timestamp_id IS NOT NULL
        AND purchase_timestamp_id IS NOT NULL
        AND purchase_timestamp_id > estimated_delivery_timestamp_id
    )
