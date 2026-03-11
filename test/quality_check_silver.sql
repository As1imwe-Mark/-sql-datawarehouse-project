--- 1. Detecting quality issues in the data silver schema

--- Check for null and duplicates in primary keys
--- Expect: no results
SELECT cst_id,COUNT(*) FROM silver.crm_cust_info GROUP BY cst_id HAVING  COUNT(*) > 1 OR cst_id IS NULL;

--- Check for unwanted spaces in string values
--- Expect: no results
SELECT cst_firstname FROM silver.crm_cust_info 
WHERE cst_firstname != TRIM(cst_firstname);

SELECT cst_gndr FROM silver.crm_cust_info 
WHERE  cst_gndr != TRIM(cst_gndr);

SELECT cst_lastname FROM silver.crm_cust_info 
WHERE  cst_lastname != TRIM(cst_lastname);

SELECT cst_marital_status,cst_key FROM silver.crm_cust_info 
WHERE  cst_marital_status != TRIM(cst_marital_status);

SELECT cst_key FROM silver.crm_cust_info 
WHERE  cst_key != TRIM(cst_key); 

--- Data Starndardization and consistency
SELECT DISTINCT cst_gndr FROM silver.crm_cust_info;
SELECT DISTINCT cst_marital_status FROM silver.crm_cust_info;

SELECT * FROM silver.crm_prd_info;

--- SLIVER.CRM_SALES_DETAILS
--- 1. Detecting quality issues in the data silver schema

--- Check for null and duplicates in primary keys
--- Expect: no results
SELECT sls_ord_num,COUNT(*) FROM silver.crm_sales_details GROUP BY sls_ord_num HAVING  COUNT(*) > 1 OR sls_ord_num IS NULL;

--- check for invalid dates 
--- Expect: no results
 
 SELECT NULLIF(sls_order_dt,0) AS sls_order_dt FROM bronze.crm_sales_details
 WHERE sls_order_dt <=0 OR LEN(sls_order_dt) !=8 OR sls_order_dt > 20500101 OR sls_order_dt < 19000101

 SELECT * FROM bronze.crm_sales_details WHERE sls_order_dt > sls_ship_dt OR sls_order_dt > sls_due_dt;

--- cHECK Data consistency: between Sales, Quantity,and Price
--- >> SALES = QUANTITY * PRICE
--- >> Values must not be NULL, Zero or Negative
SELECT DISTINCT sls_sales,sls_quantity,sls_price FROM silver.crm_sales_details 
WHERE sls_sales != (sls_quantity * sls_price) OR sls_sales <= 0 OR sls_sales IS NULL
OR sls_price IS NULL OR sls_quantity IS NULL OR sls_price <= 0 OR sls_quantity <= 0 ORDER BY sls_sales, sls_quantity,sls_price;


--- SILVER.CRP_CUST_AZ12 TABLE 

--- check for out_of_range Dates
SELECT DISTINCT bdate FROM bronze.erp_cust_az12 WHERE bdate <'1924-01-01' OR bdate > GETDATE()
