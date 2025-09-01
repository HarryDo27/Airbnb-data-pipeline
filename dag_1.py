import os
import logging
import pandas as pd
import shutil
from datetime import datetime, timedelta
from psycopg2.extras import execute_values
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

#########################################################
#
#   DAG Settings
#
#########################################################

dag_default_args = {
    'owner': 'bde',
    'start_date': datetime.now() - timedelta(days=6),
    'email': [],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'depends_on_past': False,
    'wait_for_downstream': False,
}

dag = DAG(
    dag_id='load_bronze_tables',
    default_args=dag_default_args,
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    concurrency=5
)

#########################################################
#
#   Load Environment Variables
#
#########################################################
LOCAL_DATA_PATH = "/home/airflow/gcs/data"

#########################################################
#
#   Custom Logic for Operators
#
#########################################################

def load_data_to_table(table_name, file_path, columns):
    def load_func(**kwargs):
        # Setup Postgres connection
        ps_pg_hook = PostgresHook(postgres_conn_id="postgres")
        conn_ps = ps_pg_hook.get_conn()

        # Check if the file exists
        if not os.path.exists(file_path):
            logging.info(f"No {file_path} file found.")
            return None

        # Generate dataframe by reading the CSV file
        df = pd.read_csv(file_path)

        if len(df) > 0:
            # Ensure column names match exactly as the CSV
            values = df[columns].to_dict('split')['data']
            logging.info(values)

            insert_sql = f"""
                        INSERT INTO bronze.{table_name}({', '.join(columns)})
                        VALUES %s
                        """
            execute_values(conn_ps.cursor(), insert_sql, values, page_size=len(df))
            conn_ps.commit()

            # Move the processed file to the archive folder
            archive_folder = os.path.join(os.path.dirname(file_path), 'archive')
            if not os.path.exists(archive_folder):
                os.makedirs(archive_folder)
            shutil.move(file_path, os.path.join(archive_folder, os.path.basename(file_path)))
        return None

    return load_func


# Load tasks for each table

import_load_listing_task = PythonOperator(
    task_id="import_load_listing_05_2020",
    python_callable=load_data_to_table(
        table_name="listing",
        file_path=os.path.join(LOCAL_DATA_PATH, "05_2020.csv"),
        columns=["LISTING_ID", "SCRAPE_ID", "SCRAPED_DATE", "HOST_ID", "HOST_NAME",
                 "HOST_SINCE", "HOST_IS_SUPERHOST", "HOST_NEIGHBOURHOOD", "LISTING_NEIGHBOURHOOD",
                 "PROPERTY_TYPE", "ROOM_TYPE", "ACCOMMODATES", "PRICE", "HAS_AVAILABILITY",
                 "AVAILABILITY_30", "NUMBER_OF_REVIEWS", "REVIEW_SCORES_RATING",
                 "REVIEW_SCORES_ACCURACY", "REVIEW_SCORES_CLEANLINESS", "REVIEW_SCORES_CHECKIN",
                 "REVIEW_SCORES_COMMUNICATION", "REVIEW_SCORES_VALUE"]
    ),
    provide_context=True,
    dag=dag
)

import_load_census_g01_task = PythonOperator(
    task_id="import_load_census_g01",
    python_callable=load_data_to_table(
        table_name="census_g01_nsw_lga",
        file_path=os.path.join(LOCAL_DATA_PATH, "Census LGA/2016Census_G01_NSW_LGA.csv"),
        columns=[
            "LGA_CODE_2016", "Tot_P_M", "Tot_P_F", "Tot_P_P", 
            "Age_0_4_yr_M", "Age_0_4_yr_F", "Age_0_4_yr_P", 
            "Age_5_14_yr_M", "Age_5_14_yr_F", "Age_5_14_yr_P", 
            "Age_15_19_yr_M", "Age_15_19_yr_F", "Age_15_19_yr_P", 
            "Age_20_24_yr_M", "Age_20_24_yr_F", "Age_20_24_yr_P", 
            "Age_25_34_yr_M", "Age_25_34_yr_F", "Age_25_34_yr_P", 
            "Age_35_44_yr_M", "Age_35_44_yr_F", "Age_35_44_yr_P", 
            "Age_45_54_yr_M", "Age_45_54_yr_F", "Age_45_54_yr_P", 
            "Age_55_64_yr_M", "Age_55_64_yr_F", "Age_55_64_yr_P", 
            "Age_65_74_yr_M", "Age_65_74_yr_F", "Age_65_74_yr_P", 
            "Age_75_84_yr_M", "Age_75_84_yr_F", "Age_75_84_yr_P", 
            "Age_85ov_M", "Age_85ov_F", "Age_85ov_P", 
            "Counted_Census_Night_home_M", "Counted_Census_Night_home_F", "Counted_Census_Night_home_P", 
            "Count_Census_Nt_Ewhere_Aust_M", "Count_Census_Nt_Ewhere_Aust_F", "Count_Census_Nt_Ewhere_Aust_P", 
            "Indigenous_psns_Aboriginal_M", "Indigenous_psns_Aboriginal_F", "Indigenous_psns_Aboriginal_P", 
            "Indig_psns_Torres_Strait_Is_M", "Indig_psns_Torres_Strait_Is_F", "Indig_psns_Torres_Strait_Is_P", 
            "Indig_Bth_Abor_Torres_St_Is_M", "Indig_Bth_Abor_Torres_St_Is_F", "Indig_Bth_Abor_Torres_St_Is_P", 
            "Indigenous_P_Tot_M", "Indigenous_P_Tot_F", "Indigenous_P_Tot_P", 
            "Birthplace_Australia_M", "Birthplace_Australia_F", "Birthplace_Australia_P", 
            "Birthplace_Elsewhere_M", "Birthplace_Elsewhere_F", "Birthplace_Elsewhere_P", 
            "Lang_spoken_home_Eng_only_M", "Lang_spoken_home_Eng_only_F", "Lang_spoken_home_Eng_only_P", 
            "Lang_spoken_home_Oth_Lang_M", "Lang_spoken_home_Oth_Lang_F", "Lang_spoken_home_Oth_Lang_P", 
            "Australian_citizen_M", "Australian_citizen_F", "Australian_citizen_P", 
            "Age_psns_att_educ_inst_0_4_M", "Age_psns_att_educ_inst_0_4_F", "Age_psns_att_educ_inst_0_4_P", 
            "Age_psns_att_educ_inst_5_14_M", "Age_psns_att_educ_inst_5_14_F", "Age_psns_att_educ_inst_5_14_P", 
            "Age_psns_att_edu_inst_15_19_M", "Age_psns_att_edu_inst_15_19_F", "Age_psns_att_edu_inst_15_19_P", 
            "Age_psns_att_edu_inst_20_24_M", "Age_psns_att_edu_inst_20_24_F", "Age_psns_att_edu_inst_20_24_P", 
            "Age_psns_att_edu_inst_25_ov_M", "Age_psns_att_edu_inst_25_ov_F", "Age_psns_att_edu_inst_25_ov_P", 
            "High_yr_schl_comp_Yr_12_eq_M", "High_yr_schl_comp_Yr_12_eq_F", "High_yr_schl_comp_Yr_12_eq_P", 
            "High_yr_schl_comp_Yr_11_eq_M", "High_yr_schl_comp_Yr_11_eq_F", "High_yr_schl_comp_Yr_11_eq_P", 
            "High_yr_schl_comp_Yr_10_eq_M", "High_yr_schl_comp_Yr_10_eq_F", "High_yr_schl_comp_Yr_10_eq_P", 
            "High_yr_schl_comp_Yr_9_eq_M", "High_yr_schl_comp_Yr_9_eq_F", "High_yr_schl_comp_Yr_9_eq_P", 
            "High_yr_schl_comp_Yr_8_belw_M", "High_yr_schl_comp_Yr_8_belw_F", "High_yr_schl_comp_Yr_8_belw_P", 
            "High_yr_schl_comp_D_n_g_sch_M", "High_yr_schl_comp_D_n_g_sch_F", "High_yr_schl_comp_D_n_g_sch_P", 
            "Count_psns_occ_priv_dwgs_M", "Count_psns_occ_priv_dwgs_F", "Count_psns_occ_priv_dwgs_P", 
            "Count_Persons_other_dwgs_M", "Count_Persons_other_dwgs_F", "Count_Persons_other_dwgs_P"
        ]
    ),
    provide_context=True,
    dag=dag
)

import_load_census_g02_task = PythonOperator(
    task_id="import_load_census_g02",
    python_callable=load_data_to_table(
        table_name="census_g02_nsw_lga",
        file_path=os.path.join(LOCAL_DATA_PATH, "Census LGA/2016Census_G02_NSW_LGA.csv"),
        columns=["LGA_CODE_2016", "Median_age_persons", "Median_mortgage_repay_monthly",
                 "Median_tot_prsnl_inc_weekly", "Median_rent_weekly", 
                 "Median_tot_fam_inc_weekly", "Average_num_psns_per_bedroom",
                 "Median_tot_hhd_inc_weekly", "Average_household_size"]
    ),
    provide_context=True,
    dag=dag
)

import_load_lga_code_task = PythonOperator(
    task_id="import_load_lga_code",
    python_callable=load_data_to_table(
        table_name="nsw_lga_code",
        file_path=os.path.join(LOCAL_DATA_PATH, "NSW_LGA/NSW_LGA_CODE.csv"),
        columns=["LGA_CODE", "LGA_NAME"]
    ),
    provide_context=True,
    dag=dag
)

import_load_lga_suburb_task = PythonOperator(
    task_id="import_load_lga_suburb",
    python_callable=load_data_to_table(
        table_name="nsw_lga_suburb",
        file_path=os.path.join(LOCAL_DATA_PATH, "NSW_LGA/NSW_LGA_SUBURB.csv"),
        columns=["LGA_NAME", "SUBURB_NAME"]
    ),
    provide_context=True,
    dag=dag
)

# Task Dependencies
import_load_listing_task >> import_load_census_g01_task
import_load_census_g01_task >> import_load_census_g02_task
import_load_census_g02_task >> import_load_lga_code_task
import_load_lga_code_task >> import_load_lga_suburb_task
