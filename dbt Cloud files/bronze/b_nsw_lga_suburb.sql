{{
    config(
        unique_key='suburb_name',
        alias='nsw_lga_suburb'
    )
}}

select * from {{ source('raw', 'nsw_lga_suburb') }}
