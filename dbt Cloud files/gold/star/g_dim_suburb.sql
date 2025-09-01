{{
    config(
        unique_key='suburb_name',
        alias='g_dim_suburb'
    )
}}

WITH source AS (
    SELECT 
        s.suburb_name,
        s.lga_name,
        c.lga_code
    FROM {{ ref('s_lga_suburb') }} s
    LEFT JOIN {{ ref('s_lga_code') }} c ON s.lga_name = c.lga_name
)

SELECT *
FROM source

