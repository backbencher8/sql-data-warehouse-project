-- DDL script for silver layers table
-- ================== Creating silver_crm_customer_info:  It store data cleaned from bronze_crm_customer_info================
CREATE TABLE IF NOT EXISTS silver_crm_customer_info(
	  customer_id int,
    customer_key VARCHAR(50),
    customer_firstname VARCHAR(50),
    customer_lastname VARCHAR(50),
    customer_marital_status varchar(50),
    customer_gender varchar(50),
    customer_create_date DATE,
    dwh_create_date  timestamp
);

-- ======================== Creating silver_crm_prd_info:  It store data cleaned from bronze_crm_customer_info================

CREATE TABLE IF NOT EXISTS silver_crm_prd_info(
	  prd_id int,
    cat_id varchar(50),
    prd_key VARCHAR(50),
    prd_name VARCHAR(50),
    prd_cost int,
    prd_line varchar(50),
    prd_start_dt date,
    prd_end_dt date,
    dwh_create_date timestamp
);
