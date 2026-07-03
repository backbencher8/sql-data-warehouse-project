-- =========================================== This file is to clean data in bronze_crm_product_info and inserting data into silver_crm_product_info =======================================================
INSERT INTO silver_crm_prd_info(
	prd_id,
    cat_id,
    prd_key,
    prd_name,
    prd_cost,
    prd_line,
    prd_start_dt,
    prd_end_dt
)
SELECT 
    prd_id,
    REPLACE(SUBSTRING(prd_key,1,5),'-','_') AS cat_id,
    SUBSTRING(prd_key,7,LENGTH(prd_key)) AS prd_key,
    prd_name,
    prd_cost,
    CASE UPPER(TRIM(prd_line))
        WHEN 'M' THEN 'Mountain'
        WHEN 'R' THEN 'Road'
        WHEN 'S' THEN 'Other Sales'
        WHEN 'T' THEN 'Touring'
        ELSE 'Unknown'
    END AS prd_line,
    CAST(prd_start_dt AS DATE) AS prd_start_dt,
    
    CAST(
        DATE_SUB(
            LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt),
            INTERVAL 1 DAY
        ) AS DATE
    ) AS prd_end_dt
FROM bronze_crm_prd_info;
