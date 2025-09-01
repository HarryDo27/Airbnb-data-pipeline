{{
    config(
        unique_key='lga_code_2016',
        alias='census_g02_cleaned'
    ) 
}}

WITH source AS (
    SELECT 
        CAST(lga_code_2016 AS TEXT) AS lga_code_2016,
        NULLIF(median_age_persons, 'NaN')::INTEGER AS median_age_persons,
        NULLIF(median_mortgage_repay_monthly, 'NaN')::NUMERIC AS median_mortgage_repay_monthly,
        NULLIF(median_tot_prsnl_inc_weekly, 'NaN')::NUMERIC AS median_tot_prsnl_inc_weekly,
        NULLIF(median_rent_weekly, 'NaN')::NUMERIC AS median_rent_weekly,
        NULLIF(median_tot_fam_inc_weekly, 'NaN')::NUMERIC AS median_tot_fam_inc_weekly,
        NULLIF(average_num_psns_per_bedroom, 'NaN')::NUMERIC AS average_num_psns_per_bedroom,
        NULLIF(median_tot_hhd_inc_weekly, 'NaN')::NUMERIC AS median_tot_hhd_inc_weekly,
        NULLIF(average_household_size, 'NaN')::NUMERIC AS average_household_size
    FROM {{ ref('b_census_g02_nsw_lga') }}
),

-- Calculate medians and averages for required columns
summary_stats AS (
    SELECT
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY median_age_persons) AS median_median_age_persons,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY median_mortgage_repay_monthly) AS median_median_mortgage_repay_monthly,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY median_tot_prsnl_inc_weekly) AS median_median_tot_prsnl_inc_weekly,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY median_rent_weekly) AS median_median_rent_weekly,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY median_tot_fam_inc_weekly) AS median_median_tot_fam_inc_weekly,
        AVG(average_num_psns_per_bedroom) AS avg_average_num_psns_per_bedroom,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY median_tot_hhd_inc_weekly) AS median_median_tot_hhd_inc_weekly,
        AVG(average_household_size) AS avg_average_household_size
    FROM source
),

-- Data Cleaning Step using median and average values as defaults
cleaned AS (
    SELECT 
        COALESCE(lga_code_2016, 'unknown') AS lga_code_2016,
        COALESCE(median_age_persons, (SELECT median_median_age_persons FROM summary_stats)) AS median_age_persons,
        COALESCE(median_mortgage_repay_monthly, (SELECT median_median_mortgage_repay_monthly FROM summary_stats)) AS median_mortgage_repay_monthly,
        COALESCE(median_tot_prsnl_inc_weekly, (SELECT median_median_tot_prsnl_inc_weekly FROM summary_stats)) AS median_total_personal_income_weekly,
        COALESCE(median_rent_weekly, (SELECT median_median_rent_weekly FROM summary_stats)) AS median_rent_weekly,
        COALESCE(median_tot_fam_inc_weekly, (SELECT median_median_tot_fam_inc_weekly FROM summary_stats)) AS median_total_family_income_weekly,
        COALESCE(average_num_psns_per_bedroom, (SELECT avg_average_num_psns_per_bedroom FROM summary_stats)) AS average_number_of_persons_per_bedroom,
        COALESCE(median_tot_hhd_inc_weekly, (SELECT median_median_tot_hhd_inc_weekly FROM summary_stats)) AS median_total_household_income_weekly,
        COALESCE(average_household_size, (SELECT avg_average_household_size FROM summary_stats)) AS average_household_size
    FROM source
)

SELECT * FROM cleaned
