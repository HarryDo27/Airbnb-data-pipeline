-- PART 4a
with 

top_perform_lgas as (
	select
		code.lga_name,
		concat('LGA', code.lga_code) as lga_code,
		sum(host.estimated_revenue) as total_lga_revenue,
		'top' as perform_rank
	from dbt_hnguyen_gold.dm_host_neighbourhood host 
	left join dbt_hnguyen_gold.g_dim_lga_suburb suburb 
		on host.suburb_name = suburb.suburb_name
	left join dbt_hnguyen_gold.g_dim_lga_code code 
		on suburb.lga_name = upper(code.lga_name)
	group by code.lga_name, code.lga_code
	order by total_lga_revenue desc
	limit 3
),

lowest_perform_lgas as (
	select
		code.lga_name,
		concat('LGA', code.lga_code) as lga_code,
		sum(host.estimated_revenue) as total_lga_revenue,
		'last' as perform_rank
	from dbt_hnguyen_gold.dm_host_neighbourhood host 
	left join dbt_hnguyen_gold.g_dim_lga_suburb suburb 
		on host.suburb_name = suburb.suburb_name
	left join dbt_hnguyen_gold.g_dim_lga_code code 
		on suburb.lga_name = upper(code.lga_name)
	group by code.lga_name, code.lga_code
	order by total_lga_revenue
	limit 3
),

joined_lgas as (
	select * from top_perform_lgas
	union
	select * from lowest_perform_lgas
	order by total_lga_revenue desc
)


select 
	lga.lga_name,
	round(lga.total_lga_revenue) as total_lga_revenue,
	lga.perform_rank,
	census_g02.median_mortgage_repay_monthly,
	census_g02.median_rent_weekly,
	census_g01.age_20_to_24_male, 
	census_g01.age_20_to_24_female
from joined_lgas lga
left join dbt_hnguyen_gold.dim_census_g02 census_g02 on
	lga.lga_code = census_g02.lga_code_2016
left join dbt_hnguyen_gold.dim_census_g01 census_g01 on
	lga.lga_code = census_g01.lga_code_2016 
order by total_lga_revenue desc;

-- PART 4b

with 

top_perform_lgas as (
	select
		code.lga_name,
		concat('LGA', code.lga_code) as lga_code,
		sum(host.estimated_revenue) as total_lga_revenue
	from dbt_hnguyen_gold.dm_host_neighbourhood host 
	left join dbt_hnguyen_gold.g_dim_lga_suburb suburb 
		on host.suburb_name = suburb.suburb_name
	left join dbt_hnguyen_gold.g_dim_lga_code code 
		on suburb.lga_name = upper(code.lga_name)
	group by code.lga_name, code.lga_code
	order by total_lga_revenue desc
),


cte as 
(

select 
	lga.lga_name,
	round(lga.total_lga_revenue) as total_lga_revenue,
	census_g02.median_age_persons 
from top_perform_lgas lga
left join dbt_hnguyen_gold.dim_census_g02 census_g02 on
	lga.lga_code = census_g02.lga_code_2016
left join dbt_hnguyen_gold.dim_census_g01 census_g01 on
	lga.lga_code = census_g01.lga_code_2016 
order by total_lga_revenue desc

)

select corr(total_lga_revenue, median_age_persons) as correlation_coefficient from cte;

-- PART 4c
select
	property_type,
	room_type,
	accommodates,
	round(sum(avg_estimated_revenue)) as total_estimated_revenue_active_listing_12m,
	round(sum(total_num_of_stays)) as total_num_of_stays_12m
from dbt_hnguyen_gold.dm_property_type dpt 
group by property_type , room_type , accommodates 
order by total_estimated_revenue_active_listing_12m desc
limit 5;


-- PART 4d
select 
	gdh.host_id,
	count(distinct gdh.listing_id) as num_of_listing,
	count(distinct suburb.lga_name) as num_of_lgas
from dbt_hnguyen_gold.g_dim_host gdh
left join dbt_hnguyen_gold.g_dim_property gdp
	on gdh.listing_id = gdp.listing_id
left join dbt_hnguyen_gold.g_dim_lga_suburb suburb
	on upper(gdp.listing_neighbourhood) = suburb.suburb_name 
group by host_id 
having count(distinct gdh.listing_id) > 1
order by num_of_listing desc