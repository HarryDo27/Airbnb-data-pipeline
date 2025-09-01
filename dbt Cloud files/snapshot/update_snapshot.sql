-- Populate the new column dbt_valid_to with values from the existing table
UPDATE silver.host_snapshot AS t1
SET dbt_valid_to = t2.next_date + INTERVAL '1 month'
FROM (
    SELECT 
        host_hash, 
        scraped_date, 
        LEAD(scraped_date) OVER (PARTITION BY host_hash ORDER BY scraped_date) AS next_date
    FROM silver.host_snapshot
) AS t2
WHERE t1.host_hash = t2.host_hash
  AND t1.scraped_date = t2.scraped_date;
 
-- Populate the new column dbt_valid_to with values from the existing table
UPDATE silver.property_snapshot AS t1
SET dbt_valid_to = t2.next_date + INTERVAL '1 month'
FROM (
    SELECT 
        property_id, 
        scraped_date, 
        LEAD(scraped_date) OVER (PARTITION BY property_id ORDER BY scraped_date) AS next_date
    FROM silver.property_snapshot
) AS t2
WHERE t1.property_id = t2.property_id
  AND t1.scraped_date = t2.scraped_date;