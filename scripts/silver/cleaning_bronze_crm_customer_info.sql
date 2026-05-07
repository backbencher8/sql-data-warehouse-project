-- =========================================== This file is to clean data in bronze_crm_customer_info and inserting data into silver_crm_customer_info =======================================================
INSERT into silver_crm_customer_info(
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
    customer_firstname AS cst_firstname,
    customer_lastname AS cst_lastname,
    case 
		when upper(Trim(customer_marital_status))='S' then 'Single'
		 when upper(Trim(customer_marital_status))='M' then 'Married'
         else 'Unknown'
	End customer_marital_status,
    case 
		when upper(Trim(customer_gender))='F' then 'Female'
		 when upper(Trim(customer_gender))='M' then 'Male'
         else 'Unknown'
	End customer_gender,
    customer_create_date
FROM (
    SELECT *, 
           ROW_NUMBER() OVER (
               PARTITION BY customer_id 
               ORDER BY customer_create_date DESC
           ) AS flag_last
    FROM bronze_crm_customer_info
    WHERE customer_id IS NOT NULL
) t
WHERE flag_last = 1;
