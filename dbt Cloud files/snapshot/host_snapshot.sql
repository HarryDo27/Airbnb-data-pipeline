{% snapshot host_snapshot %}

{{
    config(
        unique_key='host_hash',
        strategy='timestamp',
        updated_at='SCRAPED_DATE'
    )
}}

WITH source_data AS (
    SELECT DISTINCT
        CASE
            WHEN SCRAPED_DATE ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_DATE(SCRAPED_DATE, 'YYYY-MM-DD')
            ELSE NULL
        END AS SCRAPED_DATE,
        CAST(HOST_ID AS INTEGER) AS HOST_ID,
        md5(host_name) AS host_hash,
        COALESCE(CAST(host_name AS TEXT), 'Unknown') AS host_name,

        CASE
            WHEN HOST_SINCE ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_DATE(HOST_SINCE, 'DD/MM/YYYY')
            WHEN HOST_SINCE ~ '^\d{1}/\d{2}/\d{4}$' THEN TO_DATE(HOST_SINCE, 'D/MM/YYYY')
            WHEN HOST_SINCE ~ '^\d{2}/\d{1}/\d{4}$' THEN TO_DATE(HOST_SINCE, 'DD/M/YYYY')
            WHEN HOST_SINCE ~ '^\d{1}/\d{1}/\d{4}$' THEN TO_DATE(HOST_SINCE, 'D/M/YYYY')
            ELSE NULL
        END AS HOST_SINCE,

        CASE 
            WHEN HOST_IS_SUPERHOST = 't' THEN TRUE
            WHEN HOST_IS_SUPERHOST = 'f' THEN FALSE
            ELSE NULL
        END AS HOST_IS_SUPERHOST,

        CASE 
            WHEN HOST_NEIGHBOURHOOD = 'NaN' THEN 'Unknown'
            ELSE INITCAP(LOWER(CAST(HOST_NEIGHBOURHOOD AS TEXT)))
        END AS HOST_NEIGHBOURHOOD
    FROM {{ ref('b_listing') }}
)

SELECT *
FROM source_data

{% endsnapshot %}
