{{ config(
    materialized='table',
    unique_key='order_id',
    post_hook=[
        "ALTER TABLE {{ this }} ADD PRIMARY KEY (order_id)",
        "Alter table {{ this }} add constraint fk_fact_orders_customer_id foreign key (customer_id) references {{ ref('dim_customers') }} (customer_id)",
        "Alter table {{ this }} add constraint fk_fact_orders_purchase_date_id foreign key (purchase_date_id) references {{ ref('dim_date') }} (date_id)",
        "Alter table {{ this }} add constraint fk_fact_orders_approved_date_id foreign key (approved_date_id) references {{ ref('dim_date') }} (date_id)",
        "Alter table {{ this }} add constraint fk_fact_orders_delivered_carrier_date_id foreign key (delivered_carrier_date_id) references {{ ref('dim_date') }} (date_id)",
        "Alter table {{ this }} add constraint fk_fact_orders_delivered_customer_date_id foreign key (delivered_customer_date_id) references {{ ref('dim_date') }} (date_id)",
        "Alter table {{ this }} add constraint fk_fact_orders_estimated_delivery_date_id foreign key (estimated_delivery_date_id) references {{ ref('dim_date') }} (date_id)"
    ]
) }}

with base_orders as (
    select distinct
        so."order_id" as order_id,
        sc."customer_unique_id" as customer_id,
        "order_status" as tatus,
        nullif(so."order_purchase_timestamp", '') as purchase_timestamp_id,
        nullif(so."order_approved_at", '') as approved_timestamp_id,
        nullif(so."order_delivered_carrier_date", '') as delivered_carrier_timestamp_id,
        nullif(so."order_delivered_customer_date", '') as delivered_customer_timestamp_id,
        nullif(so."order_estimated_delivery_date", '') as estimated_delivery_timestamp_id
    from {{ source('staging', 'STG_ORDERS')}} so
    left join {{ source('staging', 'STG_CUSTOMERS') }} sc
        on so."customer_id" = sc."customer_id"
    where "order_id" is not null
),

payment_summary AS (
    SELECT
        oi."order_id" AS order_id,
        SUM(oi."price") AS total_order_amount,
        LISTAGG(
            DISTINCT 
            CASE 
                WHEN op."payment_type" = 'boleto' THEN 'Bank Transfer'
                WHEN op."payment_type" = 'credit_card' THEN 'Credit Card'
                WHEN op."payment_type" = 'debit_card' THEN 'Debit Card'
                ELSE 'Other'
            END,
            ', '
        ) AS payment_type
    FROM {{ source('staging', 'STG_ORDER_ITEMS') }} oi
    JOIN {{ source('staging', 'STG_ORDER_PAYMENTS') }} op
        ON oi."order_id" = op."order_id"
    GROUP BY oi."order_id"

),

validated_orders AS (
    SELECT
        bo.*,
        ps.total_order_amount,
        ps.payment_type
    FROM base_orders bo
    LEFT JOIN payment_summary ps
        ON bo.order_id = ps.order_id
    WHERE 1=1
        AND (
                approved_timestamp_id IS NULL 
                OR purchase_timestamp_id <= approved_timestamp_id
            )
        AND (
                delivered_carrier_timestamp_id IS NULL 
                OR approved_timestamp_id IS NULL 
                OR approved_timestamp_id <= delivered_carrier_timestamp_id
            )
        AND (
                delivered_customer_timestamp_id IS NULL 
                OR delivered_carrier_timestamp_id IS NULL 
                OR delivered_carrier_timestamp_id <= delivered_customer_timestamp_id
            )
        AND (
                estimated_delivery_timestamp_id IS NULL 
                OR purchase_timestamp_id <= estimated_delivery_timestamp_id
            )

),

final_orders as (
    select
        vo.*,
        TO_CHAR(TRY_TO_TIMESTAMP(vo.purchase_timestamp_id), 'YYYYMMDDHH24MI')::BIGINT AS purchase_date_id,
        TO_CHAR(TRY_TO_TIMESTAMP(vo.approved_timestamp_id), 'YYYYMMDDHH24MI')::BIGINT AS approved_date_id,
        TO_CHAR(TRY_TO_TIMESTAMP(vo.delivered_carrier_timestamp_id), 'YYYYMMDDHH24MI')::BIGINT AS delivered_carrier_date_id,
        TO_CHAR(TRY_TO_TIMESTAMP(vo.delivered_customer_timestamp_id), 'YYYYMMDDHH24MI')::BIGINT AS delivered_customer_date_id,
        TO_CHAR(TRY_TO_TIMESTAMP(vo.estimated_delivery_timestamp_id), 'YYYYMMDDHH24MI')::BIGINT AS estimated_delivery_date_id
    from validated_orders vo
)


select * from final_orders