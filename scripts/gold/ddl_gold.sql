--- These scripts create the different views in the gold layer with some business logic
--- ready for integration in projects because they are derived from the clean silver tables
--- We have the fact_sales view, dim_customers view and dim_products view creating the STAR schema

--- DIMENSION CUSTOMERS VIEW IN THE GOLD LAYER

CREATE VIEW gold.dim_customers AS
SELECT 
ROW_NUMBER() OVER(ORDER BY cst_key) AS customer_key,
ci.cst_id AS customer_id ,
ci.cst_key AS customer_number,
ci.cst_firstname AS first_name,
ci.cst_lastname AS last_name,
la.cntry AS country,
CASE WHEN ci.cst_gndr !='n/a' THEN ci.cst_gndr --- crm is the master for Gender Info
ELSE COALESCE(ca.gen,'n/a') END AS gender,
ci.cst_marital_status AS marital_status,
ca.bdate AS birthdate,
ci.cst_create_date AS creation_date
FROM silver.crm_cust_info AS ci
LEFT JOIN silver.erp_cust_az12 AS ca
ON ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 AS la 
ON ci.cst_key = la.cid;

--- DIMENSION PRODUCTS VIEW IN GOLD LAYER

--- filter out all the history data 
CREATE VIEW gold.dim_products AS
SELECT
ROW_NUMBER() OVER(ORDER BY pi_.prd_start_dt,pi_.prd_key) AS product_key,
pi_.prd_id AS product_id,
pi_.prd_key AS product_number,
pi_.prd_nm AS product_name,
pi_.cat_id AS category_id,
pc.cat AS category,
pc.subcat AS subcategory,
pc.maintenance,
pi_.prd_cost AS cost,
pi_.prd_line AS product_line,
pi_.prd_start_dt AS start_date
FROM silver.crm_prd_info AS pi_
LEFT JOIN silver.erp_px_cat_g1v2 AS pc
ON pi_.cat_id = pc.id
WHERE pi_.prd_end_dt IS NULL;


--- FACT SALES VIEW IN GOLD LAYER

CREATE VIEW gold.fact_sales AS
SELECT
sd.sls_ord_num AS order_number,
dp.product_key,
dc.customer_key,
sd.sls_order_dt AS order_date,
sd.sls_ship_dt AS shipping_date,
sd.sls_due_dt AS due_date,
sd.sls_sales AS sales_amount,
sd.sls_quantity AS quantity,
sd.sls_price AS price
FROM silver.crm_sales_details AS sd
LEFT JOIN gold.dim_products AS dp
ON sd.sls_prd_key = DP.product_number
LEFT JOIN gold.dim_customers AS dc
ON sd.sls_cust_id = dc.customer_id;
