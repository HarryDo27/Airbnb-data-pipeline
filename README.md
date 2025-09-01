# Building ELT Data Pipelines with Airflow and dbt

## Project Overview
The objective was to design and implement production-ready ELT pipelines using **Apache Airflow** and **dbt Cloud**, processing Sydney’s Airbnb listing data (May 2020 – April 2021) together with Census and Local Government Area (LGA) data from the Australian Bureau of Statistics.

The project follows the **Medallion architecture**:
- **Bronze**: raw data ingestion and storage  
- **Silver**: cleaned and transformed data with Slowly Changing Dimensions (SCD Type 2) snapshots  
- **Gold**: curated star schema and data marts to support analytical insights  

---

## Data Sources
- **Airbnb**: Monthly listings data (May 2020 – April 2021) from [Inside Airbnb](https://insideairbnb.com/get-the-data/)  
- **Census**: NSW demographic and socio-economic data from the [Australian Bureau of Statistics](https://www.abs.gov.au/census/find-census-data/datapacks)  
- **NSW_LGA**: Geographic and administrative mapping data for Local Government Areas  

---

## Tools Used
- **Google Cloud Platform (GCP)** – hosting and storage  
- **Airflow (Google Cloud Composer)** – data ingestion orchestration  
- **dbt Cloud** – data transformations across Bronze, Silver, and Gold layers  
- **PostgreSQL (DBeaver)** – data warehouse and schema management  

---

## Project Structure

### Part 1 – Data Ingestion & Preparation
- Established connections between GCP and PostgreSQL (via DBeaver).  
- Created **Bronze schema** with raw tables for Airbnb listings, Census, and LGA data.  
- Built Airflow DAGs to automate ingestion of CSV files into Bronze tables.  

### Part 2 – Data Warehouse with dbt
- Designed transformations in **Silver layer**, including cleaning, type casting, and SCD Type 2 snapshots (host, neighbourhood, property).  
- Built **Gold layer** with:
  - **Star schema**: one fact table (`g_fact_listing`) and six dimension tables.  
  - **Data marts**:  
    - `dm_listing_neighbourhood`: insights by neighbourhood (active listing rates, revenue, review scores).  
    - `dm_property_type`: insights by property and room types (price distribution, superhost rates, stays).  
    - `dm_host_neighbourhood`: insights by host neighbourhood and LGA (revenue per host, host counts).  

### Part 3 – End-to-End Orchestration
- Updated Airflow DAG to integrate **dbt runs** after each ingestion step.  
- Sequential monthly data loads to preserve chronological order.  
- Snapshots updated with `snapshot_update.sql` to maintain historical integrity.  

### Part 4 – Ad-hoc Analysis
- Intended to explore demographic differences, correlations between median age and revenue, and revenue vs. mortgage repayments.  
- Not fully completed due to delays in managing snapshots and sequential data loading (pipeline ran successfully for May–July 2020).  

---

## Challenges
- Managing **snapshot updates** with SCD Type 2 proved time-consuming.  
- Pipeline orchestration delays prevented full completion of Part 4.  
- Encountered issues with sequential month-by-month data loading.  

---

## Conclusion
Despite challenges, the project successfully demonstrated how to integrate Airflow and dbt Cloud for ELT pipeline design using the Medallion architecture. Data was structured across Bronze, Silver, and Gold layers, enabling star schemas and data marts for Airbnb analytical insights. The framework provides a scalable foundation for future improvements and ad-hoc analyses.

---

## How to Run
1. Clone repository and configure your GCP environment.  
2. Upload DAG scripts (e.g., `dag_1.py`, `dag1_3.py`) to Airflow in Cloud Composer.  
3. Place raw CSV files (Airbnb, Census, LGA) into the appropriate GCS data bucket.  
4. Trigger DAGs in Airflow UI to ingest data into **Bronze** schema.  
5. Run dbt Cloud transformations for **Silver** and **Gold** layers.  
6. Query data marts (`dm_listing_neighbourhood`, `dm_property_type`, `dm_host_neighbourhood`) for insights.  
