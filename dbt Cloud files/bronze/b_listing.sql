{{
    config(
        unique_key='listing_id',
        alias='listing'
    )
}}

select * from {{ source('raw', 'listing') }}