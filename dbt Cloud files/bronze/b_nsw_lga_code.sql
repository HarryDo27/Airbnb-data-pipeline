{{
    config(
        unique_key='lga_code',
        alias='nsw_lga_code'
    )
}}

select * from {{ source('raw', 'nsw_lga_code') }}