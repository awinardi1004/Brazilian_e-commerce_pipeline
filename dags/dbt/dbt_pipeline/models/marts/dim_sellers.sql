{{
    config(
        materializerd = 'table',
        post_hook="ALTER TABLE {{ this }} ADD PRIMARY KEY (seller_id)"
    )
}}

select distinct
    "seller_id" as seller_id,
    "seller_city" as city,
    "seller_state" as state,
    "seller_zip_code_prefix" as zip_code
from {{ source('staging', 'STG_SELLERS')}}