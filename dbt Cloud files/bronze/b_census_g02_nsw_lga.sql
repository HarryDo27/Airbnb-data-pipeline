{{
    config(
        unique_key='lga_code_2016',
        alias='census_g02_nsw_lga'
    )
}}

select * from {{ source('raw', 'census_g02_nsw_lga') }}