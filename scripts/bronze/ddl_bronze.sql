/* DDL Script to Create Bronze layer tables
=============================================================================================
Script Purpose: 
  This script creates the tables in the bronze layer. It checks if the table with the specified name alreaady exists or not and if it is not there then the table is created.
  It also inserts the data from the files present in you device. If you are in MYSQL there may occur some error while inseting data from local file. Try using GPT to solve those errors.
-- ********************************** Choosing the database **********************************
USE data_warehouse;

-- ********************************** Create customer_information table **********************************
CREATE TABLE IF NOT EXISTS bronze_crm_customer_info(
	customer_id int,
    customer_key VARCHAR(50),
    customer_firstname VARCHAR(50),
    customer_lastname VARCHAR(50),
    customer_marital_status varchar(50),
    customer_gender varchar(50),
    customer_create_date DATE 
);

-- ********************************** Create product_information table **********************************
CREATE TABLE IF NOT EXISTS bronze_crm_prd_info(
	prd_id int,
    prd_key VARCHAR(50),
    prd_name VARCHAR(50),
    prd_cost int,
    prd_line varchar(50),
    prd_start_dt datetime,
    prd_end_dt datetime
);	

-- ********************************** Create sales_details table **********************************
CREATE TABLE IF NOT EXISTS bronze_sales_details(
	sls_ord_num varchar(50),
    sls_prd_key VARCHAR(50),
    sls_cust_id int,
    sls_order_dt int,
    sls_ship_dt int,
    sls_due_dt int,
    sls_sales int,
    sls_quantity int,
    sls_price int    
);	

CREATE TABLE IF NOT EXISTS bronze_erp_loc_a101(
	cid varchar(50),
    cntry varchar(50)
);

CREATE TABLE IF NOT EXISTS bronze_erp_cust_az12(
	cid varchar(50),
    bdate date,
    gen varchar(50)
);

CREATE TABLE IF NOT EXISTS bronze_erp_px_cat_g1v2(
	id varchar(50),
    cat varchar(50),
    subcat varchar(50),
    maintanence varchar(50)
);

SET GLOBAL local_infile = 1;

-- ********************************** Loading data of customer into bronze_crm_customer_info table from csv file **********************************

Truncate table bronze_crm_customer_info;
LOAD DATA LOCAL INFILE 'C:/Users/HP/Desktop/sql-data-warehouse-project-main/datasets/source_crm/cust_info.csv' -- paste the path of the file from your pc
INTO TABLE bronze_crm_customer_info
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(
    @cst_id,
    @cst_key,
    @cst_firstname,
    @cst_lastname,
    @cst_marital_status,
    @cst_gndr,
    @cst_create_date
)
SET
    customer_id = @cst_id,
    customer_key = TRIM(@cst_key),
    customer_firstname = TRIM(@cst_firstname),
    customer_lastname = TRIM(@cst_lastname),
    customer_marital_status = TRIM(@cst_marital_status),
    customer_gender = TRIM(@cst_gndr),
    customer_create_date = @cst_create_date;
    
select * from bronze_crm_customer_info;
Select count(*) from bronze_crm_customer_info;

-- ********************************** Loading data of product into bronze_crm_prd_info table from csv file **********************************

TRUNCATE TABLE bronze_crm_prd_info;
LOAD DATA LOCAL INFILE 'C:/Users/HP/Desktop/sql-data-warehouse-project-main/datasets/source_crm/prd_info.csv' -- paste the path of the file from your pc
INTO TABLE bronze_crm_prd_info
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(
	@prd_id,
    @prd_key,
    @prd_nm,
    @prd_cost,
    @prd_line,
    @prd_start_dt,
    @prd_end_dt
)
SET
	prd_id = @prd_id,
    prd_key = trim(@prd_key),
    prd_name = trim(@prd_nm),
    prd_cost = trim(@prd_cost),
    prd_line = trim(@prd_line),
    prd_start_dt = trim(@prd_start_dt),
    prd_end_dt = trim(@prd_end_dt);

select * from bronze_crm_prd_info;
select count(*) from bronze_crm_prd_info;
show tables;

-- ********************************** Loading data of product into bronze_sales_details table from csv file **********************************
TRUNCATE TABLE bronze_sales_details;

LOAD DATA LOCAL INFILE 'C:/Users/HP/Desktop/sql-data-warehouse-project-main/datasets/source_crm/sales_details.csv' -- paste the path of the file from your pc
INTO TABLE bronze_sales_details
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
 (
	@sls_ord_num,
    @sls_prd_key,
    @sls_cust_id,
    @sls_order_dt,
    @sls_ship_dt,
    @sls_due_dt,
    @sls_sales,
    @sls_quantity,
    @sls_price
 )
 SET
	sls_ord_num = @sls_ord_num,
    sls_prd_key = @sls_prd_key,
    sls_cust_id = @sls_cust_id,
    sls_order_dt = @sls_order_dt,
    sls_ship_dt = @sls_ship_dt,
    sls_due_dt = @sls_due_dt,
    sls_sales = @sls_sales,
    sls_quantity = @sls_quantity,
    sls_price = @sls_price;
    
    
select * from bronze_sales_details;

-- ********************************** Loading data of product into bronze_crm_prd_info table from csv file **********************************
Truncate table bronze_erp_cust_az12;
LOAD DATA LOCAL INFILE 'C:/Users/HP/Desktop/sql-data-warehouse-project-main/datasets/source_erp/cust_az12.csv' -- paste the path of the file from your pc
INTO TABLE bronze_erp_cust_az12
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(
	@CID,
    @BDATE,
    @GEN
)
SET
	cid = @CID,
    bdate = @bdate,
    gen = @gen;
    
SELECT * FROM bronze_erp_cust_az12;

-- ********************************** Loading data of product into bronze_erp_loc_a101 table from csv file **********************************
TRUNCATE TABLE bronze_erp_loc_a101;
LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/LOC_A101.csv' -- paste the path of the file from your pc
INTO TABLE bronze_erp_loc_a101
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(
	@CID,
    @CNTRY
)
SET
	cid = @CID,
    cntry = @CNTRY ;
SELECT * FROM bronze_erp_loc_a101;


-- ********************************** Loading data of product into bronze_erp_loc_a101 table from csv file **********************************
TRUNCATE TABLE bronze_erp_loc_a101;
LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/PX_CAT_G1V2.csv' -- paste the path of the file from your pc
INTO TABLE bronze_erp_px_cat_g1v2
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(
	@ID,
    @CAT,
    @SUBCAT,
    @MAINTENANCE
)
SET
	id = @ID,
    cat= @CAT,
    subcat = @SUBCAT,
	maintanence = @MAINTENANCE;
    
SELECT * FROM bronze_erp_px_cat_g1v2;




