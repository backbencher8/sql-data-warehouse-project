Insert into silver_erp_cust_az12 (
cid, bdate, gen
)
select DISTINCT
CASE WHEN cid like 'NAS%' THEN substr(cid, 4, length(cid))
	Else cid
End cid,
CASE WHEN bdate > current_date() then null
else bdate
end as bdate,
CASE
    WHEN UPPER(TRIM(REPLACE(gen, CHAR(13), ''))) IN ('F', 'FEMALE') THEN 'Female'
    WHEN UPPER(TRIM(REPLACE(gen, CHAR(13), ''))) IN ('M', 'MALE') THEN 'Male'
    ELSE 'n/a'
END AS gen
from bronze_erp_cust_az12;


-- ************************ cleaning data  ********************
SELECT distinct
gen,
CASE
    WHEN UPPER(TRIM(REPLACE(gen, CHAR(13), ''))) IN ('F', 'FEMALE') THEN 'Female'
    WHEN UPPER(TRIM(REPLACE(gen, CHAR(13), ''))) IN ('M', 'MALE') THEN 'Male'
    ELSE 'n/a'
END AS gen
FROM bronze_erp_cust_az12;

SELECT
    gen,
    TRIM(gen) AS trimmed,
    UPPER(TRIM(gen)) AS cleaned
FROM bronze_erp_cust_az12;

SELECT DISTINCT
    gen,
    LENGTH(gen) AS len,
    UPPER(TRIM(gen)) AS cleaned,
    length(UPPER(TRIM(gen))) as new_len
FROM bronze_erp_cust_az12;

SELECT
    gen,
    LENGTH(gen) AS length,
    HEX(gen) AS hex_value
FROM bronze_erp_cust_az12; 

select gen from bronze_erp_cust_az12;

select * from bronze_erp_cust_az12;

select bdate from silver_erp_cust_az12 where bdate<'1924-01-01' or bdate>current_date() order by bdate;

select DISTINCT gen from silver_erp_cust_az12 order by gen desc;

