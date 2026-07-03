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


-- ********************************** Create silver_sales_details table **********************************
CREATE TABLE IF NOT EXISTS silver_sales_details(
	sls_ord_num varchar(50),
    sls_prd_key VARCHAR(50),
    sls_cust_id int,
    sls_order_dt int,
    sls_ship_dt int,
    sls_due_dt int,
    sls_sales int,
    sls_quantity int,
    sls_price int 
    dwh_create_date timestamp
);	


-- ********************************** Create silver_erp_loc_a101 **********************************
CREATE TABLE IF NOT EXISTS silver_erp_loc_a101(
	cid varchar(50),
    cntry varchar(50)
    dwh_create_date timestamp
);

CREATE TABLE IF NOT EXISTS silver_erp_cust_az12(
	cid varchar(50),
    bdate date,
    gen varchar(50)
    dwh_create_date timestamp
);

CREATE TABLE IF NOT EXISTS silver_erp_px_cat_g1v2(
	id varchar(50),
    cat varchar(50),
    subcat varchar(50),
    maintanence varchar(50)
    dwh_create_date timestamp
);
