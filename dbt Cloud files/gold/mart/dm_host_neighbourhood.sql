WITH source AS (
    SELECT
        dh.HOST_NEIGHBOURHOOD,
        TO_CHAR(CAST(dh.SCRAPED_DATE AS DATE), 'MM-YYYY') AS MONTH_YEAR, 
        f.LISTING_ID,
        f.PRICE,
        f.AVAILABILITY_30,
        dh.HOST_ID
    FROM {{ ref('g_fact_listing') }} F
    LEFT JOIN {{ ref('g_dim_host') }} DH ON F.HOST_ID = DH.HOST_ID
),

lga_mapping AS (
    SELECT
        ds.SUBURB_NAME,
        ds.LGA_NAME
    FROM {{ ref ('g_dim_suburb')}} ds
),

aggregated AS (
    SELECT
        CASE 
            WHEN source.HOST_NEIGHBOURHOOD = 'Unknown' THEN 'Unknown'
            WHEN source.HOST_NEIGHBOURHOOD = 'Overseas' THEN 'Overseas'
            ELSE lga_mapping.LGA_NAME
        END AS HOST_NEIGHBOURHOOD_LGA,
        source.MONTH_YEAR,
        COUNT(DISTINCT source.HOST_ID) AS NUMBER_OF_DISTINCT_HOSTS,
        SUM(source.PRICE * (30 - source.AVAILABILITY_30)) AS ESTIMATED_REVENUE
    FROM source
    LEFT JOIN lga_mapping 
    ON source.HOST_NEIGHBOURHOOD = lga_mapping.SUBURB_NAME
    GROUP BY 
        CASE 
            WHEN source.HOST_NEIGHBOURHOOD = 'Unknown' THEN 'Unknown'
            WHEN source.HOST_NEIGHBOURHOOD = 'Overseas' THEN 'Overseas'
            ELSE lga_mapping.LGA_NAME
        END, 
        source.MONTH_YEAR
)

SELECT
        HOST_NEIGHBOURHOOD_LGA,
        MONTH_YEAR,
        NUMBER_OF_DISTINCT_HOSTS,
        ESTIMATED_REVENUE,
        (ESTIMATED_REVENUE::NUMERIC / NULLIF(NUMBER_OF_DISTINCT_HOSTS, 0)) AS ESTIMATED_REVENUE_PER_HOST
FROM aggregated
ORDER BY HOST_NEIGHBOURHOOD_LGA, MONTH_YEAR
