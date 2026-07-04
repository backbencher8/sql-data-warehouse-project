-- ********************************** Checking sales details *********************************
-- ================== Check invalid dates ===============
SELECT
nullif (sls_order_dt,0) sls_order_dt
FROM bronze_sales_details
where sls_order_dt<=0 
or length(sls_order_dt) != 8
or sls_order_dt>20500101 
or sls_order_dt<19900101;

-- ==================== check date for orders ==================
select
*
from bronze_sales_details
where sls_ship_dt<sls_order_dt or sls_due_dt<sls_order_dt;

-- ============ checking business rules for sales = quantity * price, sales !=null or -ve ===============
select DISTINCTsilver_sales_detailssilver_crm_customer_info
sls_sales as old_sales,
sls_quantity,
sls_price as old_sls_price,
case when sls_sales is null or sls_sales<=0 or sls_sales != sls_quantity * abs(sls_price)
	then sls_quantity * abs(sls_price)
    else sls_sales
end as sls_sales,
case when sls_price is null or sls_price <= 0
	then round(sls_sales/nullif(sls_quantity,0))
    else round(sls_price)
end as sls_price
from bronze_sales_details
where sls_sales != sls_quantity * sls_price
or sls_sales <= 0 or sls_sales is null
or sls_price <= 0 or sls_price is null
or sls_quantity <= 0 or sls_quantity is null
ORDER BY sls_sales, sls_quantity, sls_price;



-- ********************************** Checking customer details details *********************************


