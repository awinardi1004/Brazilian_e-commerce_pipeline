{{
    config(
        materialized='table',
        post_hook="ALTER TABLE {{ this }} ADD PRIMARY KEY (product_id)"
    )
}}

with base_product as (
    select distinct
        p."product_id" as product_id,
        pc."product_category_name_english" as category_name,
        p."product_weight_g" as weight_g,
        p."product_length_cm" as length_cm,
        p."product_height_cm" as height_cm,
        p."product_width_cm" as width_cm
    from {{ source('staging', 'STG_PRODUCTS') }} p
    left join {{ source('staging', 'STG_PRODUCT_CATEGORY_NAME_TRANSLATION') }} pc
        on p."product_category_name" = pc."product_category_name"
    where p."product_id" is not null
),

transformed_products as (
    select
        product_id,
        coalesce(nullif(trim(category_name), ''), 'Unknown') as category_name,
        weight_g,
        length_cm,
        height_cm,
        width_cm
    from base_product
)

select distinct * 
from transformed_products
