{{
    config(
        unique_key='LISTING_ID',
        alias='s_fact_listing'
    )
}}

WITH source AS (
    SELECT 
        CAST(LISTING_ID AS INTEGER) AS LISTING_ID,
        CAST(SCRAPE_ID AS BIGINT) AS SCRAPE_ID,
        CAST(HOST_ID AS INTEGER) AS HOST_ID,
        CASE
            WHEN PRICE = 'NaN' THEN NULL
            ELSE CAST(PRICE AS FLOAT)  
        END AS PRICE,
        CASE
            WHEN AVAILABILITY_30 = 'NaN' THEN NULL
            ELSE CAST(AVAILABILITY_30 AS INTEGER)
        END AS AVAILABILITY_30,
        CASE
            WHEN NUMBER_OF_REVIEWS = 'NaN' THEN NULL
            ELSE CAST(NUMBER_OF_REVIEWS AS INTEGER)
        END AS NUMBER_OF_REVIEWS,
        CASE
            WHEN REVIEW_SCORES_RATING = 'NaN' THEN NULL
            ELSE CAST(REVIEW_SCORES_RATING AS NUMERIC)
        END AS REVIEW_SCORES_RATING,
        CASE
            WHEN REVIEW_SCORES_ACCURACY = 'NaN' THEN NULL
            ELSE CAST(REVIEW_SCORES_ACCURACY AS NUMERIC)
        END AS REVIEW_SCORES_ACCURACY,
        CASE
            WHEN REVIEW_SCORES_CLEANLINESS = 'NaN' THEN NULL
            ELSE CAST(REVIEW_SCORES_CLEANLINESS AS NUMERIC)
        END AS REVIEW_SCORES_CLEANLINESS,
        CASE
            WHEN REVIEW_SCORES_CHECKIN = 'NaN' THEN NULL
            ELSE CAST(REVIEW_SCORES_CHECKIN AS NUMERIC)
        END AS REVIEW_SCORES_CHECKIN,
        CASE
            WHEN REVIEW_SCORES_COMMUNICATION = 'NaN' THEN NULL
            ELSE CAST(REVIEW_SCORES_COMMUNICATION AS NUMERIC)
        END AS REVIEW_SCORES_COMMUNICATION,
        CASE
            WHEN REVIEW_SCORES_VALUE = 'NaN' THEN NULL
            ELSE CAST(REVIEW_SCORES_VALUE AS NUMERIC)
        END AS REVIEW_SCORES_VALUE
    FROM {{ ref('b_listing') }}
),

-- Subqueries to Calculate Medians for Each Numeric Column
median_values AS (
    SELECT
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY PRICE) AS median_price,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY AVAILABILITY_30) AS median_availability_30,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY NUMBER_OF_REVIEWS) AS median_number_of_reviews,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY REVIEW_SCORES_RATING) AS median_review_scores_rating,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY REVIEW_SCORES_ACCURACY) AS median_review_scores_accuracy,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY REVIEW_SCORES_CLEANLINESS) AS median_review_scores_cleanliness,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY REVIEW_SCORES_CHECKIN) AS median_review_scores_checkin,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY REVIEW_SCORES_COMMUNICATION) AS median_review_scores_communication,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY REVIEW_SCORES_VALUE) AS median_review_scores_value
    FROM source
),

-- Data Cleaning Step Using Median Values
cleaned AS (
    SELECT 
        LISTING_ID,
        SCRAPE_ID,
        HOST_ID,
        COALESCE(PRICE, (SELECT median_price FROM median_values)) AS price,
        COALESCE(AVAILABILITY_30, (SELECT median_availability_30 FROM median_values)) AS availability_30,
        COALESCE(NUMBER_OF_REVIEWS, (SELECT median_number_of_reviews FROM median_values)) AS number_of_reviews,
        COALESCE(REVIEW_SCORES_RATING, (SELECT median_review_scores_rating FROM median_values)) AS review_scores_rating,
        COALESCE(REVIEW_SCORES_ACCURACY, (SELECT median_review_scores_accuracy FROM median_values)) AS review_scores_accuracy,
        COALESCE(REVIEW_SCORES_CLEANLINESS, (SELECT median_review_scores_cleanliness FROM median_values)) AS review_scores_cleanliness,
        COALESCE(REVIEW_SCORES_CHECKIN, (SELECT median_review_scores_checkin FROM median_values)) AS review_scores_checkin,
        COALESCE(REVIEW_SCORES_COMMUNICATION, (SELECT median_review_scores_communication FROM median_values)) AS review_scores_communication,
        COALESCE(REVIEW_SCORES_VALUE, (SELECT median_review_scores_value FROM median_values)) AS review_scores_value
    FROM source
    WHERE PRICE <> 0
)

SELECT * 
FROM cleaned
