{{
    config(
        unique_key='lga_code',
        alias='g_dim_lga'
    )
}}

WITH source AS (

    SELECT * FROM {{ ref('s_lga_code') }}

)

SELECT *
FROM source
