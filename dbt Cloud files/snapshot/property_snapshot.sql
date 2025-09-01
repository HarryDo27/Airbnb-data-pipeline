{% snapshot property_snapshot %}

{{
    config(
        unique_key='property_id',
        strategy='timestamp',
        updated_at='SCRAPED_DATE'
    )
}}

WITH source_data AS (
    SELECT DISTINCT
        md5(PROPERTY_TYPE) AS property_id,
        CAST(LISTING_ID AS INTEGER) AS LISTING_ID,
        CASE
            WHEN SCRAPED_DATE ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_DATE(SCRAPED_DATE, 'YYYY-MM-DD')
            ELSE NULL
        END AS SCRAPED_DATE,
        CASE 
            WHEN LISTING_NEIGHBOURHOOD = 'NaN' THEN 'Unknown'
            ELSE INITCAP(LOWER(CAST(LISTING_NEIGHBOURHOOD AS TEXT)))
        END AS LISTING_NEIGHBOURHOOD,

        COALESCE(CAST(PROPERTY_TYPE AS TEXT), 'Unknown') AS PROPERTY_TYPE,
        COALESCE(CAST(ROOM_TYPE AS TEXT), 'Unknown') AS ROOM_TYPE,

        CASE 
            WHEN HAS_AVAILABILITY = 't' THEN TRUE
            WHEN HAS_AVAILABILITY = 'f' THEN FALSE
            ELSE NULL
        END AS HAS_AVAILABILITY,

        CASE
            WHEN ACCOMMODATES = 'NaN' THEN NULL
            ELSE CAST(ACCOMMODATES AS NUMERIC)
        END AS ACCOMMODATES
    FROM {{ ref('b_listing') }}
)

SELECT *
FROM source_data

{% endsnapshot %}
