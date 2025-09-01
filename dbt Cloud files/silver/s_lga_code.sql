{{ 
    config(
        unique_key='LGA_CODE',
        alias='s_lga_code'
    ) 
}}

WITH source AS (
    SELECT 
        CAST(LGA_CODE AS INTEGER) AS LGA_CODE,
        INITCAP(LOWER(CAST(LGA_NAME AS TEXT))) AS LGA_NAME
    FROM {{ ref('b_nsw_lga_code') }}
),

cleaned AS (
    SELECT
        COALESCE(LGA_CODE, 0) AS lga_code, 
        COALESCE(LGA_NAME, 'unknown') AS lga_name
    FROM source
)

SELECT * FROM cleaned
