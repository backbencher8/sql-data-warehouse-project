/* Script Purpose:
    This script creates views for the Gold layer in the data warehouse. 
    The Gold layer represents the final dimension and fact tables (Star Schema)

    This view performs transformations and combines data from the Silver layer 
    to produce a clean, enriched, and business-ready dataset.

Usage:
    - These views can be queried directly for analytics and reporting.
	*/
create view gold_dim_product as 
select 
	Row_number() over (order by pr.prd_start_dt, pr.prd_key) as product_key,
	pr.prd_id as product_id,
	pr.prd_key as product_number,
	pr.prd_name  as product_name,
    pr.cat_id as category_id,
    pc.cat as category,
	pc.subcat as sub_category,
    pc.maintanence,
    pr.prd_cost as cost,
	pr.prd_line as product_line,
	pr.prd_start_dt as start_date
from silver_crm_prd_info pr
left join silver_erp_px_cat_g1v2 pc on  
pr.cat_id = pc.id
where pr.prd_end_dt is Null;

select * from gold_dim_product;
