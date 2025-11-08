{{
    config(
        materialized='table',
        post_hook=[
            "ALTER TABLE {{ this }} ADD PRIMARY KEY (order_id, product_id)",
            "Alter table {{ this }} add constraint fk_fact_order_items_order_id foreign key (order_id) references {{ ref('fact_orders') }} (order_id)",
            "Alter table {{ this }} add constraint fk_fact_order_items_product_id foreign key (product_id) references {{ ref('dim_products') }} (product_id)",
            "Alter table {{ this }} add constraint fk_fact_order_items_seller_id foreign key (seller_id) references {{ ref('dim_sellers') }} (seller_id)",
            "Alter table {{ this }} add constraint fk_fact_order_items_shipping_limit_date_id foreign key (shipping_limit_date_id) references {{ ref('dim_date') }} (date_id)"
        ]
    )
}}

WITH aggregated_items AS (
    SELECT
        "order_id" as order_id,
        "product_id" as product_id,
        "seller_id" as seller_id,
        COUNT(*) AS quantity,
        SUM("price") AS total_price,
        SUM("freight_value") AS total_freight,
        TO_CHAR(CAST("shipping_limit_date" AS TIMESTAMP), 'YYYYMMDDHH24MI')::BIGINT AS shipping_limit_date_id
    FROM {{ source('staging', 'STG_ORDER_ITEMS') }}
    GROUP BY "order_id", "product_id", "seller_id", "shipping_limit_date"
),

valid_orders AS (
    SELECT order_id
    FROM {{ ref('fact_orders') }}
),

final AS (
    SELECT
        ai.order_id,
        ai.product_id,
        ai.seller_id,
        ai.quantity,
        ai.total_price,
        ai.total_freight,
        ai.shipping_limit_date_id
    FROM aggregated_items ai
    INNER JOIN valid_orders vo ON ai.order_id = vo.order_id
)

SELECT * FROM final
