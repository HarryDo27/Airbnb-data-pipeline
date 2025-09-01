{{
    config(
        unique_key='listing_id',
        alias='g_fact_listing'
    )
}}

WITH

source AS (

    SELECT * FROM {{ ref('s_fact_listing') }}

)

SELECT * FROM source