-- **************************** This script is to create procedure that loads data from bronze layer to silver layer ******************************** 
DROP PROCEDURE IF EXISTS load_silver;

DELIMITER $$

CREATE PROCEDURE load_silver()
BEGIN

    /*=========================================================
      Load silver_crm_customer_info
    =========================================================*/

    TRUNCATE TABLE silver_crm_customer_info;

    INSERT INTO silver_crm_customer_info (
        customer_id,
        customer_key,
        customer_firstname,
        customer_lastname,
        customer_marital_status,
        customer_gender,
        customer_create_date
    )
    SELECT
        customer_id,
        customer_key,
        customer_firstname,
        customer_lastname,
        CASE
            WHEN UPPER(TRIM(customer_marital_status)) = 'S' THEN 'Single'
            WHEN UPPER(TRIM(customer_marital_status)) = 'M' THEN 'Married'
            ELSE 'Unknown'
        END,
        CASE
            WHEN UPPER(TRIM(customer_gender)) = 'F' THEN 'Female'
            WHEN UPPER(TRIM(customer_gender)) = 'M' THEN 'Male'
            ELSE 'Unknown'
        END,
        customer_create_date
    FROM (
        SELECT *,
               ROW_NUMBER() OVER(
                   PARTITION BY customer_id
                   ORDER BY customer_create_date DESC
               ) AS flag_last
        FROM bronze_crm_customer_info
        WHERE customer_id IS NOT NULL
    ) t
    WHERE flag_last = 1;


    /*=========================================================
      Load silver_erp_cust_az12
    =========================================================*/

    TRUNCATE TABLE silver_erp_cust_az12;

    INSERT INTO silver_erp_cust_az12 (
        cid,
        bdate,
        gen
    )
    SELECT DISTINCT
        CASE
            WHEN cid LIKE 'NAS%' THEN SUBSTR(cid,4)
            ELSE cid
        END,
        CASE
            WHEN bdate > CURRENT_DATE() THEN NULL
            ELSE bdate
        END,
        CASE
            WHEN UPPER(TRIM(REPLACE(gen,CHAR(13),''))) IN ('F','FEMALE')
                THEN 'Female'
            WHEN UPPER(TRIM(REPLACE(gen,CHAR(13),''))) IN ('M','MALE')
                THEN 'Male'
            ELSE 'n/a'
        END
    FROM bronze_erp_cust_az12;


    /*=========================================================
      Load silver_erp_loc_a101
    =========================================================*/

    TRUNCATE TABLE silver_erp_loc_a101;

    INSERT INTO silver_erp_loc_a101 (
        cid,
        cntry
    )
    SELECT
        REPLACE(cid,'-',''),
        CASE
            WHEN TRIM(REPLACE(cntry,CHAR(13),''))='DE'
                THEN 'Germany'
            WHEN TRIM(REPLACE(cntry,CHAR(13),'')) IN ('US','USA')
                THEN 'United States'
            WHEN TRIM(REPLACE(cntry,CHAR(13),''))='' OR cntry IS NULL
                THEN 'n/a'
            ELSE TRIM(REPLACE(cntry,CHAR(13),''))
        END
    FROM bronze_erp_loc_a101;


    /*=========================================================
      Load silver_erp_px_cat_g1v2
    =========================================================*/

    TRUNCATE TABLE silver_erp_px_cat_g1v2;

    INSERT INTO silver_erp_px_cat_g1v2 (
        id,
        cat,
        subcat,
        maintanence
    )
    SELECT
        id,
        TRIM(cat),
        TRIM(subcat),
        TRIM(maintanence)
    FROM bronze_erp_px_cat_g1v2;


    /*=========================================================
      Load silver_sales_details
    =========================================================*/

    TRUNCATE TABLE silver_sales_details;

    INSERT INTO silver_sales_details (
        sls_ord_num,
        sls_prd_key,
        sls_cust_id,
        sls_order_dt,
        sls_ship_dt,
        sls_due_dt,
        sls_sales,
        sls_quantity,
        sls_price
    )
    SELECT
        sls_ord_num,
        sls_prd_key,
        sls_cust_id,

        CASE
            WHEN sls_order_dt=0
                OR LENGTH(CAST(sls_order_dt AS CHAR))<>8
            THEN NULL
            ELSE STR_TO_DATE(CAST(sls_order_dt AS CHAR),'%Y%m%d')
        END,

        CASE
            WHEN sls_ship_dt=0
                OR LENGTH(CAST(sls_ship_dt AS CHAR))<>8
            THEN NULL
            ELSE STR_TO_DATE(CAST(sls_ship_dt AS CHAR),'%Y%m%d')
        END,

        CASE
            WHEN sls_due_dt=0
                OR LENGTH(CAST(sls_due_dt AS CHAR))<>8
            THEN NULL
            ELSE STR_TO_DATE(CAST(sls_due_dt AS CHAR),'%Y%m%d')
        END,

        CASE
            WHEN sls_sales IS NULL
                 OR sls_sales<=0
                 OR sls_sales<>sls_quantity*ABS(sls_price)
            THEN sls_quantity*ABS(sls_price)
            ELSE sls_sales
        END,

        sls_quantity,

        CASE
            WHEN sls_price IS NULL
                 OR sls_price<=0
            THEN sls_sales/NULLIF(sls_quantity,0)
            ELSE sls_price
        END

    FROM bronze_sales_details;

END$$

DELIMITER ;
