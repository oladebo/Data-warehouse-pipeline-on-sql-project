/*
=======================================================================================================
                                Some of the Quality Checks Perform on Data 
========================================================================================================
For Scripts puposes:
  This scripts performs various quality checks for the following data quality check and consistency.
  * Unwanted spaces in the data strings field columns
  * Invalid data range or order, nul or duplicates primary key
  * Data Structure standardization  & Consistency of data related to each columns field

Usage Notes:
  * When you run these checks after loading "Silver Layer"
  * Also audit/ investigate  and resloved any diffrencies  you find out during the checks
============================================================================================================
*/

-- =====================================================================
--  SILVER LAYER -(CLEAN & TRANSFORM DATA) CRM_PROD TABLE - CRM_PROD_INFO
-- =====================================================================


-- Check for Unwanted spaces
-- Expectation: No Result

SELECT prod_name
FROM silver.crm_prod_info
WHERE prod_name != TRIM(prod_name);

SELECT * FROM silver.crm_prod_info;

-- For NUll or Negative Numbers
-- Expectation: NO Result

SELECT prod_cost
FROM silver.crm_prod_info
WHERE prod_cost < 0 or prod_cost IS NULL;

-- Data Standardization And Consistency

SELECT DISTINCT prod_line
FROM silver.crm_prod_info;

-- Check for invalid Date Order

SELECT * FROM silver.crm_prod_info
WHERE prod_end_date < prod_start_date



-- =====================================================================
--  SILVER LAYER -(CLEAN & TRANSFORM DATA) CRM_PROD TABLE - CRM_CUST_INFO
-- =====================================================================



  --- Expectation: No Result

SELECT * FROM silver.crm_cust_info;

SELECT cust_key
SELECT cust_firstname
FROM silver.crm_cust_info
WHERE cust_firstname != TRIM(cust_firstname);

-- Data Standardation and consistency

SELECT DISTINCT cust_gender
FROM silver.crm_cust_info;


-- Quality Check of the bronze layer to table before transfer then to silver layer

-- 1. Check for the Nulls and Duplicates in the primary keys
-- expectation: No result
-- 2. Check for unwanted spaces in strings


SELECT cust_id, COUNT(*) FROM silver.crm_cust_info
GROUP BY cust_id
HAVING COUNT(*) > 1 OR cust_id is Null

SELECT cust_id, COUNT(*) FROM silver.crm_cust_info
Group BY cust_id
HAVING COUNT (*) = 1


-- Check for unwanted Spacess
FROM bronze.crm_cust_info
WHERE cust_key !=TRIM(cust_key);



-- Expectation is Zero
SELECT cust_firstname
FROM silver.crm_cust_info
WHERE cust_firstname != TRIM(cust_firstname);

-- Check for unwanted spaces
-- Expectation: Zero (no result)

-- =====================================================================
--  SILVER LAYER -(CLEAN & TRANSFORM DATA) CRM_PROD TABLE - CRM_PROD_INFO
-- =====================================================================


  
--DATA Cleaning Bronze_crm_prod.info i.e 


SELECT * FROM silver.crm_prod_info;

SELECT
prod_id, COUNT(*)
FROM silver.crm_prod_info
GROUP BY prod_id
HAVING COUNT(*)> 1 OR prod_id IS NULL

-- Check for unwanted spaces
-- Expectaion:

SELECT prod_name
FROM silver.crm_prod_info
WHERE prod_name !=TRIM(prod_name);




-- Check for NUll or negative name
-- Expectation: No Result

SELECT prod_name
FROM silver.crm_prod_info
WHERE prod_name !=TRIM(prod_name);


SELECT prod_cost
FROM silver.crm_prod_info
WHERE prod_cost < 0 OR prod_cost IS NULL

-- Data Standaardization & Consistency

SELECT DISTINCT prod_line
FROM silver.crm_prod_info

-- Check for invalid date order

SELECT * 
FROM silver.crm_prod_info
WHERE prod_end_date < prod_start_date



-- Tranformation performance checker

SELECT * FROM silver.crm_prod_info;

-- Quality Check of the bronze layer to table before transfer then to silver layer

-- 1. Check for the Nulls and Duplicates in the primary keys
-- expectation: No result
-- 2. Check for unwanted spaces in strings


SELECT prod_id, COUNT(*) FROM silver.crm_prod_info
GROUP BY prod_id
HAVING COUNT(*) > 1 OR prod_id is Null

SELECT cust_id, COUNT(*) FROM silver.crm_cust_info
Group BY cust_id
HAVING COUNT (*) = 1


-- Check for unwanted Spacess
-- Expectation is Zero
SELECT prod_id
FROM silver.crm_prod_info
WHERE cust_firstname != TRIM(cust_firstname);

-- Check for unwanted spaces
-- Expectation: Zero (no result)

SELECT prod_name
FROM silver.crm_prod_info
WHERE prod_name != TRIM(prod_name);



-- ==================================================================================
--  SILVER LAYER -(CLEAN & TRANSFORM DATA) CRM_SALES_DETAILS TABLE - CRM_SALES_DETAILS
-- ===================================================================================


-- Data Standardization & Consistency

SELECT *
FROM silver.crm_sales_details
WHERE sales_order_date < sales_ship_date OR sales_order_date > sales_due_date;


-- =====================================================================
--  SILVER LAYER -(CLEAN & TRANSFORM DATA) CUST_INFO TABLE - CRM_CUST_INFO
-- =====================================================================



SELECT cust_key
FROM silver.crm_cust_info
WHERE cust_key != TRIM(cust_key);

-- Data Standardization & Consistency

SELECT DISTINCT cust_gender
FROM silver.crm_cust_info;
