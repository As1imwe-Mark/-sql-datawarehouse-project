--- This stored procedure script does the ETL process and then loads the data into the silver schema
--- It first truncates the tables before bulk inserting so it is advisable to first back up all your data.

CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
BEGIN TRY

--- SILVER.CRM_CUST_INFO TABLE

--- Removing duplicates
--- Cleaning spaces from string fields
--- Data Starndardization and consistency
TRUNCATE TABLE silver.crm_cust_info;

INSERT INTO silver.crm_cust_info(cst_id, cst_key,cst_firstname,cst_lastname,cst_marital_status,cst_gndr,cst_create_date)
SELECT cst_id, cst_key, TRIM(cst_firstname) AS cst_firstname,
TRIM(cst_lastname) AS cst_lastname,
CASE WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single' 
WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married' 
ELSE 'n/a' END AS cst_marital_status,
CASE WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female' 
WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male' 
ELSE 'n/a' END AS cst_gndr,
cst_create_date  
FROM(SELECT *, ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last 
FROM bronze.crm_cust_info)t WHERE flag_last = 1 AND cst_id IS NOT NULL;


--- SILVER.CRM_PRD_INFO TABLE 

--- Removing duplicates
--- create new column for joining later
--- Cleaning spaces from string fields
--- Replace nulls in number fields with 0
--- Fix end date that is smaller than start date by using LEAD() -1
--- Data Starndardization and consistency

TRUNCATE TABLE silver.crm_prd_info;

INSERT INTO silver.crm_prd_info(prd_id,cat_id,prd_key,prd_nm,prd_cost,prd_line,prd_start_dt,prd_end_dt)
SELECT prd_id,
LEFT(REPLACE(TRIM(prd_key),'-','_'),5) AS cat_id,
SUBSTRING(TRIM(prd_key),7,LEN(prd_key)) AS prd_key,
prd_nm,
ISNULL(prd_cost,0) AS prd_cost,
CASE UPPER(TRIM(prd_line)) 
WHEN 'M' THEN 'Mountain' 
WHEN 'R' THEN 'Road'
WHEN 'T' THEN 'Touring'
WHEN 'S' THEN 'Other Sales'
ELSE 'n/a' END AS  prd_line,
CAST(prd_start_dt AS DATE) AS prd_start_dt,
CAST(LEAD(prd_start_dt) OVER(PARTITION BY prd_key ORDER BY prd_start_dt) -1 AS DATE) AS prd_end_dt
FROM bronze.crm_prd_info;


--- SILVER.CRM_SALES_DETAILS TABLE 

--- Cleaning spaces from string fields
--- Fixing negatives and  nulls in number fields
--- Fix dates and cast them from int to date
--- Data Starndardization and consistency

TRUNCATE TABLE silver.crm_sales_details;

INSERT INTO silver.crm_sales_details(sls_ord_num,sls_prd_key,sls_cust_id,sls_order_dt,sls_ship_dt,sls_due_dt,sls_price,sls_quantity,sls_sales)
SELECT 
sls_ord_num,
sls_prd_key,
sls_cust_id,
CASE WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
ELSE CAST(CAST(sls_order_dt AS VARCHAR ) AS DATE) END AS sls_order_dt,
CASE WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
ELSE CAST(CAST(sls_ship_dt AS VARCHAR ) AS DATE) END AS sls_ship_dt,
CASE WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL
ELSE CAST(CAST(sls_due_dt AS VARCHAR ) AS DATE) END AS sls_due_dt,
CASE WHEN sls_price = 0 OR sls_price IS NULL THEN (sls_sales/NULLIF(sls_quantity,0)) 
WHEN sls_price < 0 THEN ABS(sls_price) ELSE sls_price END AS sls_price,
sls_quantity,
CASE WHEN sls_sales <= 0  OR sls_sales IS NULL OR sls_sales != (sls_quantity * ABS(sls_price)) 
THEN sls_quantity * ABS(sls_price) ELSE sls_sales END AS sls_sales
FROM bronze.crm_sales_details; 



--- SILVER.ERP_CUST_AZ12 TABLE 

--- Cleaning spaces from string fields
--- Fix OUT OF SCOPE DATES
--- Data Starndardization and consistency: FIXED MIXED INPUTS IN GENDER TO STARNDARD VALUES and handled missing values

TRUNCATE TABLE silver.erp_cust_az12;

INSERT INTO silver.erp_cust_az12 (cid,bdate,gen)
SELECT
CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid,4, LEN(cid)) ELSE cid END AS 
cid,
CASE WHEN bdate > GETDATE() THEN NULL ELSE bdate END AS bdate,
CASE 
WHEN UPPER(TRIM(gen)) IN ('M','MALE') THEN 'Male'
WHEN UPPER(TRIM(gen)) IN ('F','FEMALE') THEN 'Female'
WHEN UPPER(TRIM(gen)) = NULL OR UPPER(TRIM(gen))='' THEN 'n/a' 
ELSE TRIM(gen) END AS gen
FROM bronze.erp_cust_az12;


--- SILVER.ERP_LOC_A101 TABLE 
--- Replaced - with nothing in the cid to match the cst_key in the cust_info table for proper joining
--- Data Starndardization and consistency: FIXED MIXED INPUTS IN cntry TO STARNDARD known VALUES and handled missing values

TRUNCATE TABLE silver.erp_loc_a101;

INSERT INTO silver.erp_loc_a101(cid,cntry)
SELECT 
REPLACE(TRIM(cid),'-','') AS cid,
CASE WHEN cntry IS NULL OR cntry = '' THEN 'n/a' 
WHEN cntry IN ('USA','US') THEN 'United States'
WHEN UPPER(TRIM(cntry)) = 'DE' THEN 'Germany' ELSE TRIM(cntry) END AS cntry
FROM bronze.erp_loc_a101;

--- SILVER.ERP_PX_CAT_G1V2 TABLE 

TRUNCATE TABLE silver.erp_px_cat_g1v2;

INSERT INTO silver.erp_px_cat_g1v2(id,cat,subcat,maintenance)
SELECT 
TRIM(id),
TRIM(cat) AS cat,
TRIM(subcat) AS subcat,
TRIM(maintenance) AS maintenance
FROM bronze.erp_px_cat_g1v2;
END TRY
BEGIN CATCH
PRINT'======================================================================='
	PRINT'ERROR OCCURED DURING LOADING SILVER LAYER'
	PRINT'Error Message'+ ERROR_MESSAGE()
	PRINT'Error Message'+ CAST(ERROR_NUMBER() AS NVARCHAR)
	PRINT'======================================================================='
END CATCH;
END;
