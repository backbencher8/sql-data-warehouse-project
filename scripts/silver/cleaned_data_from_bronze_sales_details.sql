-- =========================================== This file is to clean data in bronze_sales_details and inserting data into silver_sales_details =======================================================
TRUNCATE TABLE silver_sales_details;

Insert into silver_sales_details(
sls_ord_num, sls_prd_key, sls_cust_id, sls_order_dt, sls_ship_dt, sls_due_dt, sls_sales, sls_quantity, sls_price
)
SELECT
    sls_ord_num,
    sls_prd_key,
    sls_cust_id,
    CASE
        WHEN sls_order_dt = 0
             OR LENGTH(CAST(sls_order_dt AS CHAR)) != 8
        THEN NULL
        ELSE STR_TO_DATE(CAST(sls_order_dt AS CHAR), '%Y%m%d')
    END AS sls_order_dt,
    CASE
        WHEN sls_ship_dt = 0
             OR LENGTH(CAST(sls_ship_dt AS CHAR)) != 8
        THEN NULL
        ELSE STR_TO_DATE(CAST(sls_ship_dt AS CHAR), '%Y%m%d')
    END AS sls_ship_dt,
    CASE
        WHEN sls_due_dt = 0
             OR LENGTH(CAST(sls_due_dt AS CHAR)) != 8
        THEN NULL
        ELSE STR_TO_DATE(CAST(sls_due_dt AS CHAR), '%Y%m%d')
    END AS sls_due_dt,
    CASE WHEN sls_sales is NULL or sls_sales <=0 OR
sls_sales != sls_quantity * abs(sls_price)
	THEN sls_quantity * abs(sls_price)
    else sls_sales
End as sls_sales,
    sls_quantity,
    CASE WHEN sls_price is null or sls_price <=0
	THEN sls_sales/nullif(sls_quantity, 0)
    ELSE sls_price
END AS sls_price
FROM bronze_sales_details;


SELECT nullif(sls_order_dt,0) from bronze_sales_details where sls_order_dt<= 0;

-- *********************************** Checking business rule *****************************
-- ******* Sales = Quantity x Price ********* Sales shouldn't be (Null, -ve, 0) ******************
SELECT DISTINCT
sls_sales as old_sls_sales,
sls_quantity,
sls_price as old_sls_price,
CASE WHEN sls_sales is NULL or sls_sales <=0 OR
sls_sales != sls_quantity * abs(sls_price)
	THEN sls_quantity * abs(sls_price)
    else sls_sales
End as sls_sales,
CASE WHEN sls_price is null or sls_price <=0
	THEN sls_sales/nullif(sls_quantity, 0)
    ELSE sls_price
END AS sls_price
FROM bronze_sales_details
WHERE sls_sales != sls_price * sls_quantity or 
sls_sales IS NULL or sls_quantity is NULL or sls_price is null or
sls_sales <=0 or sls_quantity <=0 or sls_price <=0
ORDER BY sls_sales, sls_price, sls_quantity ;

Alter table silver_sales_details
Modify COLUMN sls_order_dt DATE;

Alter table silver_sales_details
Modify COLUMN sls_ship_dt Date;

Alter table silver_sales_details
Modify COLUMN sls_due_dt Date;

select * from silver_sales_details;

SELECT DISTINCT
sls_sales,
sls_quantity,
sls_price
FROM silver_sales_details
WHERE sls_sales != sls_price * sls_quantity or 
sls_sales IS NULL or sls_quantity is NULL or sls_price is null or
sls_sales <=0 or sls_quantity <=0 or sls_price <=0
ORDER BY sls_sales, sls_price, sls_quantity ;
