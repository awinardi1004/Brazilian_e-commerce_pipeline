{{ config(
    materialized='table',
    post_hook="ALTER TABLE {{ this }} ADD PRIMARY KEY (date_id)"
) }}

WITH base AS (
    select
        nullif("order_purchase_timestamp", '') as event_time
    from {{ source('staging', 'STG_ORDERS')}}
    where "order_purchase_timestamp" is not null

    Union

    select
        nullif("order_approved_at", '') as event_time
    from {{ source('staging', 'STG_ORDERS')}}
    where "order_approved_at" is not null 

    Union

    select
        nullif("order_delivered_carrier_date", '') as event_time
    from {{ source('staging', 'STG_ORDERS')}}
    where "order_delivered_carrier_date" is not null

    Union

    select
        nullif("order_delivered_customer_date", '') as event_time
    from {{ source('staging', 'STG_ORDERS')}}
    where "order_delivered_customer_date" is not null  

    Union

    select
        nullif("order_estimated_delivery_date", '') as event_time
    from {{ source('staging', 'STG_ORDERS')}}
    where "order_estimated_delivery_date" is not null

    Union

    select
        nullif("shipping_limit_date", '') as event_time
    from {{ source('staging', 'STG_ORDER_ITEMS')}}
    where "shipping_limit_date" is not null
),

distinct_times AS (
    SELECT DISTINCT
        TO_CHAR(CAST(event_time AS TIMESTAMP), 'YYYYMMDDHH24MI')::BIGINT AS date_id,
        DATE_TRUNC('minute', CAST(event_time AS TIMESTAMP)) AS full_datetime
    FROM base
),

date_parts AS (
    SELECT
        date_id,
        full_datetime,
        EXTRACT(YEAR FROM full_datetime) AS year,
        EXTRACT(MONTH FROM full_datetime) AS month,
        EXTRACT(DAY FROM full_datetime) AS day,
        TO_CHAR(full_datetime, 'Day') AS day_of_week,
        EXTRACT(HOUR FROM full_datetime) AS hour,
        CONCAT(EXTRACT(YEAR FROM full_datetime), 'Q', EXTRACT(QUARTER FROM full_datetime)) AS year_quarter
    FROM distinct_times
)

select * from date_parts
where date_id is not null