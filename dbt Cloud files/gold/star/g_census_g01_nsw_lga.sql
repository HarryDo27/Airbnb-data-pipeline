{{
    config(
        unique_key='lga_code_2016',
        alias='g_census_g01'
    )
}}

WITH source AS (

    SELECT * FROM {{ ref('s_census_g01_nsw_lga') }}

)
SELECT *
FROM source
