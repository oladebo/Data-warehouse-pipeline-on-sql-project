/*
===================================================================================================
                                  -- Gold Layer Quality Checks:
===================================================================================================
Scripts Purpose:
    This scripts performs quality checks to validate the data integration consistency & accuracy 
    of the "Gold Layer", and this checks ensure  the following
1. Validation of relationship in the data model for analitics and purpose
2.  The uniqueness of sorrogate keys in the dimention table 
3.  Diffrentials integrity between facts and dimention table 

Usage: 
  - Investigate and resolves any discrpancies find during the checks 
  - Run checks after loading silver layer.
=====================================================================================================
\*


================================================================================
-- Check the Gold Layer customer by using Alias
==================================================================================

SELECT cust_id, COUNT(*)FROM
(SELECT 
    cd.cust_id,
    cd.cust_key,
    cd.cust_firstname,
    cd.cust_lastname,
    cd.cust_marital_status,
    cd.cust_gender,
    cd.cust_create_date,    
    ca.birth_date,
    ca.gender,
    ls.country
FROM silver.crm_cust_info cd
LEFT JOIN silver.erp_cust_az1 ca
    ON cd.cust_key = ca.cid
LEFT JOIN silver.erp_loc_a101 ls
    ON cd.cust_key = ls.cid
)t GROUP BY cust_id
HAVING COUNT(*)>1;


-- Data Integration
SELECT DISTINCT
    cd.cust_gender,
    ca.gender,
	CASE WHEN cd.cust_gender !='n/a' THEN cd.cust_gender -- When CRM is the Master of gender details infor
		ELSE COALESCE (ca.gender, 'n/a')
	END AS new_gender
FROM silver.crm_cust_info cd
LEFT JOIN silver.erp_cust_az1 ca
    ON cd.cust_key = ca.cid
LEFT JOIN silver.erp_loc_a101 ls
    ON cd.cust_key = ls.cid
	ORDER BY 1,2;
	
	
-- Rename the columns name to well understanding columns name 
SELECT 
    cd.cust_id AS customer_id,
    cd.cust_key AS customer_number,
    cd.cust_firstname AS first_name,
    cd.cust_lastname AS last_name,
    cd.cust_marital_status AS marital_status,
    CASE WHEN cd.cust_gender !='n/a' THEN cd.cust_gender -- When CRM is the Master of gender details infor
		ELSE COALESCE (ca.gender, 'n/a')
	END AS gender,
	cd.cust_create_date AS create_date,    
    ca.birth_date AS birthdate,
   	ls.country AS country
FROM silver.crm_cust_info cd
LEFT JOIN silver.erp_cust_az1 ca
    ON cd.cust_key = ca.cid
LEFT JOIN silver.erp_loc_a101 ls
    ON cd.cust_key = ls.cid;
	

-- Meanwhile we can re_arrange columns name that is best suit us 
SELECT 
    cd.cust_id AS customer_id,
    cd.cust_key AS customer_number,
    cd.cust_firstname AS first_name,
    cd.cust_lastname AS last_name,
    cd.cust_marital_status AS marital_status,
    CASE WHEN cd.cust_gender !='n/a' THEN cd.cust_gender -- When CRM is the Master of gender details infor
		ELSE COALESCE (ca.gender, 'n/a')
	END AS gender,
	ls.country AS country,
	ca.birth_date AS birthdate,
	cd.cust_create_date AS create_date
FROM silver.crm_cust_info cd
LEFT JOIN silver.erp_cust_az1 ca
    ON cd.cust_key = ca.cid
LEFT JOIN silver.erp_loc_a101 ls
    ON cd.cust_key = ls.cid;
	
	
-- Dimention and Fact, Dimention describe infor about object incase it does'nt 
-- primary key to count on in the data warehosue and is call Sorrogate keys 
-- system generated key unique identifier assign to each records
SELECT
	ROW_NUMBER() OVER (ORDER BY cust_id) AS customer_key,
    cd.cust_id AS customer_id,
    cd.cust_key AS customer_number,
    cd.cust_firstname AS first_name,
    cd.cust_lastname AS last_name,
    cd.cust_marital_status AS marital_status,
    CASE WHEN cd.cust_gender !='n/a' THEN cd.cust_gender -- When CRM is the Master of gender details infor
		ELSE COALESCE (ca.gender, 'n/a')
	END AS gender,
	ls.country AS country,
	ca.birth_date AS birthdate,
	cd.cust_create_date AS create_date
FROM silver.crm_cust_info cd
LEFT JOIN silver.erp_cust_az1 ca
    ON cd.cust_key = ca.cid
LEFT JOIN silver.erp_loc_a101 ls
    ON cd.cust_key = ls.cid;
	
	
-- Join tha table silver.crm_prod_info and erp_px_cat_g1v2
SELECT
	pn.prod_id,
	pn.cat_id,
	pn.prod_key,
	pn.prod_name,
	pn.prod_cost,
	pn.prod_line,
	pn.prod_start_date,
	pc.category,
	pc.sub_category,
	pc.maintenance
FROM silver.crm_prod_info pn
LEFT JOIN silver.erp_px_cat_g1v2 pc
ON pn.cat_id = pc.id
WHERE pn.prod_end_date IS NULL -- Filter all the historical data

-- Data quality checks by filtering

SELECT prod_key, COUNT(*) FROM (
SELECT
	pn.prod_id,
	pn.cat_id,
	pn.prod_key,
	pn.prod_name,
	pn.prod_cost,
	pn.prod_line,
	pn.prod_start_date,
	pc.category,
	pc.sub_category,
	pc.maintenance
FROM silver.crm_prod_info pn
LEFT JOIN silver.erp_px_cat_g1v2 pc
ON pn.cat_id = pc.id
WHERE pn.prod_end_date IS NULL -- Filter all the historical data

)t GROUP BY prod_key
HAVING COUNT(*)>1 


-- Re-Arrange the product table colums orderly
SELECT
	pn.prod_id,
	pn.prod_key,
	pn.prod_name,
	pn.cat_id,
	pc.category,
	pc.sub_category,
	pc.maintenance,
	pn.prod_cost,
	pn.prod_line,
	pn.prod_start_date
FROM silver.crm_prod_info pn
LEFT JOIN silver.erp_px_cat_g1v2 pc
ON pn.cat_id = pc.id
WHERE pn.prod_end_date IS NULL -- Filter all the historical data

-- Rename the product table colums orderly better understand also generate sorrogate keys
SELECT
	ROW_NUMBER() OVER (ORDER BY pn.prod_start_date,pn.prod_key)AS prod_key,
	pn.prod_id AS product_id,
	pn.prod_key AS product_number,
	pn.prod_name AS product_name,
	pn.cat_id AS category_id,
	pc.category,
	pc.sub_category AS subcategory,
	pc.maintenance,
	pn.prod_cost AS product_cost,
	pn.prod_line AS product_line,
	pn.prod_start_date AS start_date
FROM silver.crm_prod_info pn
LEFT JOIN silver.erp_px_cat_g1v2 pc
ON pn.cat_id = pc.id
WHERE pn.prod_end_date IS NULL -- Filter all the historical data



-- Foreign Keys intergrity(Demension)

SELECT * FROM gold.fact_sales f
LEFT JOIN gold.dim_customer c
ON c.customer_key = f.customer_key
LEFT JOIN gold.dim_product p
ON p.product_key = f.product_key
WHERE p.product_key IS NULL
