-- =========================================== This file is to clean data in bronze_erp_loc_a101 and inserting data into silver_erp_loc_a101 =======================================================
TRUNCATE TABLE silver_erp_loc_a101;
Insert into silver_erp_loc_a101 (cid, cntry)
select
replace(cid,'-','') cid,
CASE
        WHEN TRIM(REPLACE(cntry, CHAR(13), '')) = 'DE' THEN 'Germany'
        WHEN TRIM(REPLACE(cntry, CHAR(13), '')) IN ('US', 'USA') THEN 'United States'
        WHEN TRIM(REPLACE(cntry, CHAR(13), '')) = '' OR cntry IS NULL THEN 'n/a'
        ELSE TRIM(REPLACE(cntry, CHAR(13), ''))
    END AS cntry
from bronze_erp_loc_a101;


select * from bronze_erp_loc_a101;

select
replace(cid,'-','') cid,
cntry
from bronze_erp_loc_a101;

SELECT DISTINCT cntry from bronze_erp_loc_a101 order by cntry;
-- Data standarization & consistency
SELECT DISTINCT
    cntry AS old_cntry,
    CASE
        WHEN TRIM(REPLACE(cntry, CHAR(13), '')) = 'DE' THEN 'Germany'
        WHEN TRIM(REPLACE(cntry, CHAR(13), '')) IN ('US', 'USA') THEN 'United States'
        WHEN TRIM(REPLACE(cntry, CHAR(13), '')) = '' OR cntry IS NULL THEN 'n/a'
        ELSE TRIM(REPLACE(cntry, CHAR(13), ''))
    END AS cntry
FROM bronze_erp_loc_a101;



select distinct cntry from silver_erp_loc_a101;

