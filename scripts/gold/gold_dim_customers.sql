/* Script Purpose:
    This script creates views for the Gold layer in the data warehouse. 
    The Gold layer represents the final dimension and fact tables (Star Schema)

    This view performs transformations and combines data from the Silver layer 
    to produce a clean, enriched, and business-ready dataset.

Usage:
    - These views can be queried directly for analytics and reporting.
	*/
Create view gold_dim_customers as 
select 
	ROW_NUMBER() OVER (order by customer_id) as customer_key,
	ci.customer_id as customer_id,
	ci.customer_key as customer_number,
	ci.customer_firstname as first_name,
	ci.customer_lastname as last_name,
    la.cntry as country,
	ci.customer_marital_status as marital_status,
	CASE
        WHEN ci.customer_gender IN ('Male', 'Female')
            THEN ci.customer_gender

        WHEN ci.customer_gender = 'Unknown'
             AND ca.gen IN ('Male', 'Female')
            THEN ca.gen

        ELSE 'n/a'
    END AS gender,
    ca.bdate as birthdate,
	ci.customer_create_date as create_date
from silver_crm_customer_info ci
left join silver_erp_cust_az12 ca
on ci.customer_key = ca.cid
left join silver_erp_loc_a101 la
on ci.customer_key = la.cid;


-- checking gender match or not 
SELECT DISTINCT
    CASE
        WHEN ci.customer_gender IN ('Male', 'Female')
            THEN ci.customer_gender

        WHEN ci.customer_gender = 'Unknown'
             AND ca.gen IN ('Male', 'Female')
            THEN ca.gen

        ELSE 'n/a'
    END AS new_gen
FROM silver_crm_customer_info ci
LEFT JOIN silver_erp_cust_az12 ca
    ON ci.customer_key = ca.cid
LEFT JOIN silver_erp_loc_a101 la
    ON ci.customer_key = la.cid
ORDER BY 1,2;

SELECT
    customer_gender,
    COUNT(*) AS total_customers
FROM silver_crm_customer_info
GROUP BY customer_gender
HAVING COUNT(*) > 1;

select distinct gender from gold_dim_customers;


