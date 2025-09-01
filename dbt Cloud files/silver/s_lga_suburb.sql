{{ 
    config(
        unique_key='SUBURB_NAME',
        alias='s_lga_suburb'
    ) 
}}

WITH source AS (
    SELECT 
        INITCAP(LOWER(CAST(SUBURB_NAME AS TEXT))) AS SUBURB_NAME,
        INITCAP(LOWER(CAST(LGA_NAME AS TEXT))) AS LGA_NAME
    FROM {{ ref('b_nsw_lga_suburb') }}
),

cleaned AS (
    SELECT
        COALESCE(LGA_NAME, 'Unknown') AS lga_name,
        COALESCE(SUBURB_NAME, 'Unknown') AS suburb_name
    FROM source 
)

SELECT * FROM cleaned
