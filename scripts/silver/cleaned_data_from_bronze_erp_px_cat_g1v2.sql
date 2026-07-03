-- =========================================== This file is to clean data in bronze_erp_px_cat_g1v2 and inserting data into silver_erp_px_cat_g1v2 =======================================================
Insert into silver_erp_px_cat_g1v2
(id, cat, subcat, maintanence)
Select
id,
cat,
subcat,
maintanence
from bronze_erp_px_cat_g1v2;

select 
* from bronze_erp_px_cat_g1v2 
where trim(cat) != cat
or trim(subcat) != subcat
or trim(maintanence) != maintanence;

select distinct maintanence from bronze_erp_px_cat_g1v2;

select * from silver_erp_px_cat_g1v2;
