/*
===============================================================================
Quality Checks
===============================================================================
Script Purpose:
    This script performs various quality checks for data consistency, accuracy,
    and standardization across the 'silver' layer. It includes checks for:
    - Null or duplicate primary keys.
    - Unwanted spaces in string fields.
    - Data standardization and consistency.
    - Invalid date ranges and orders.
    - Data consistency between related fields.

Usage Notes:
    - Run these checks after loading data into the Silver Layer.
    - Investigate and resolve any discrepancies found during the checks.
===============================================================================
*/

-- ********************************** Checking silver_crm_customer_info **********************************

-- Check for NULLs or duplicate customer IDs
SELECT
    customer_id,
    COUNT(*) AS record_count
FROM silver_crm_customer_info
GROUP BY customer_id
HAVING COUNT(*) > 1
   OR customer_id IS NULL;

-- Check for unwanted spaces
SELECT
    customer_key
FROM silver_crm_customer_info
WHERE customer_key <> TRIM(customer_key);

-- Data Standardization & Consistency
SELECT DISTINCT
    customer_marital_status
FROM silver_crm_customer_info;

SELECT DISTINCT
    customer_gender
FROM silver_crm_customer_info;


-- ********************************** Checking silver_crm_prd_info **********************************

-- Check for NULLs or duplicate product IDs
SELECT
    prd_id,
    COUNT(*) AS record_count
FROM silver_crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1
   OR prd_id IS NULL;

-- Check for unwanted spaces
SELECT
    prd_name
FROM silver_crm_prd_info
WHERE prd_name <> TRIM(prd_name);

-- Check for NULL or negative costs
SELECT
    prd_cost
FROM silver_crm_prd_info
WHERE prd_cost IS NULL
   OR prd_cost < 0;

-- Data Standardization & Consistency
SELECT DISTINCT
    prd_line
FROM silver_crm_prd_info;

-- Check for invalid date ranges
SELECT *
FROM silver_crm_prd_info
WHERE prd_end_dt < prd_start_dt;


-- ********************************** Checking silver_crm_sales_details **********************************

-- Check for NULL dates
SELECT
    sls_order_dt,
    sls_ship_dt,
    sls_due_dt
FROM silver_crm_sales_details
WHERE sls_order_dt IS NULL
   OR sls_ship_dt IS NULL
   OR sls_due_dt IS NULL;

-- Check for invalid date order
SELECT *
FROM silver_crm_sales_details
WHERE sls_order_dt > sls_ship_dt
   OR sls_order_dt > sls_due_dt;

-- Check sales consistency
SELECT DISTINCT
    sls_sales,
    sls_quantity,
    sls_price
FROM silver_crm_sales_details
WHERE sls_sales <> sls_quantity * sls_price
   OR sls_sales IS NULL
   OR sls_quantity IS NULL
   OR sls_price IS NULL
   OR sls_sales <= 0
   OR sls_quantity <= 0
   OR sls_price <= 0
ORDER BY
    sls_sales,
    sls_quantity,
    sls_price;


-- ********************************** Checking silver_erp_cust_az12 **********************************

-- Check for unrealistic birth dates
SELECT DISTINCT
    bdate
FROM silver_erp_cust_az12
WHERE bdate < '1924-01-01'
   OR bdate > CURRENT_DATE();

-- Data Standardization & Consistency
SELECT DISTINCT
    gen
FROM silver_erp_cust_az12;


-- ********************************** Checking silver_erp_loc_a101 **********************************

SELECT DISTINCT
    cntry
FROM silver_erp_loc_a101
ORDER BY cntry;


-- ********************************** Checking silver_erp_px_cat_g1v2 **********************************

-- Check for unwanted spaces
SELECT *
FROM silver_erp_px_cat_g1v2
WHERE cat <> TRIM(cat)
   OR subcat <> TRIM(subcat)
   OR maintanence <> TRIM(maintanence);

-- Data Standardization
SELECT DISTINCT
    maintanence
FROM silver_erp_px_cat_g1v2;
