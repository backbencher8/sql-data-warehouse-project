/* Script Purpose:
    This script creates views for the Gold layer in the data warehouse. 
    The Gold layer represents the final dimension and fact tables (Star Schema)

    This view performs transformations and combines data from the Silver layer 
    to produce a clean, enriched, and business-ready dataset.

Usage:
    - These views can be queried directly for analytics and reporting.
	*/

Create view gold_fact_sales as
select
sl.sls_ord_num as order_number,
pr.product_key,
cu.customer_key,
sl.sls_order_dt as order_date,
sl.sls_ship_dt as shipping_date,
sl.sls_due_dt  as due_date,
sl.sls_sales as sales_amount,
sl.sls_quantity as quantity,
sl.sls_price as price
from silver_sales_details sl
left join gold_dim_product pr
on sl.sls_prd_key = pr.product_number
left join gold_dim_customers cu
on sl.sls_cust_id = cu.customer_id;
