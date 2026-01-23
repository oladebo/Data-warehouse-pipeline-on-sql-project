-- ======================================================================================
/* Create Silver Layer and schemas and clean/Transform Load data
-- =======================================================================================
Scripts Purpose:
  This scripts create a silver layer schema table and load data "DataWarehose" to 
  clean and transform after checking if is already exist, If the schema and data 
  databese still exist,it is going to be drop and recreated. However the scripts
  sets has three schemas within database "bronze", "silver", and "gold" and this 
  second stage

Warning:
  Running this script will drop will drop the entire "DataWarehose" Database if it's exists,
  All databes will be permanently deleted. proceed with caution and ensure you have proper
  back_up before running the script
*/
-- =========================================================================================
-- After This, You insert into Silver Layer 

-- Query that is doing data transformation and cleaning

-- Clean, transform, and deduplicate customer data

-- Clean, transform, and deduplicate customer data


-- =====================================================================
--  SILVER LAYER -(CLEAN & TRANSFORM DATA) CUST TABLE - CRM_CUST_INFO
-- =====================================================================

ALTER TABLE silver.crm_cust_info
DROP CONSTRAINT crm_cust_info_cust_marital_status_check;

ALTER TABLE silver.crm_cust_info
ADD CONSTRAINT crm_cust_info_cust_marital_status_check
CHECK (cust_marital_status IN ('Married', 'Single',  'n/a'));


TRUNCATE TABLE silver.crm_cust_info;

INSERT INTO silver.crm_cust_info (
    cust_id,
    cust_key,
    cust_firstname,
    cust_lastname,
    cust_marital_status,
    cust_gender,
    cust_create_date
)
WITH deduped_customers AS (
    SELECT 
        cust_id,
        cust_key,
        cust_firstname,
        cust_lastname,
        cust_marital_status,
        cust_gender,
        cust_create_date,
        ROW_NUMBER() OVER (
            PARTITION BY cust_id
            ORDER BY 
                CASE 
                    WHEN TRIM(cust_create_date) ~ '^\d{1,2}/\d{1,2}/\d{2,4}$' 
                    THEN TO_DATE(TRIM(cust_create_date), 'MM/DD/YYYY')
                    ELSE '1900-01-01'::DATE
                END DESC,
                cust_key DESC
        ) AS rn
    FROM bronze.crm_cust_info
    WHERE cust_id IS NOT NULL  -- Early filtering of NULL cust_id
),
cleaned_data AS (
    SELECT
        cust_id,
        cust_key,
        -- Clean names - handle NULLs and empty strings
        CASE 
            WHEN TRIM(cust_firstname) = '' THEN NULL
            ELSE TRIM(cust_firstname)
        END AS cust_firstname,
        CASE 
            WHEN TRIM(cust_lastname) = '' THEN NULL
            ELSE TRIM(cust_lastname)
        END AS cust_lastname,
        
        -- Standardize marital status with better NULL handling
        CASE 
            WHEN cust_marital_status IS NULL THEN 'n/a'
            WHEN UPPER(TRIM(cust_marital_status)) = 'M' THEN 'Married'
            WHEN UPPER(TRIM(cust_marital_status)) = 'S' THEN 'Single'
            WHEN TRIM(cust_marital_status) = '' THEN 'n/a'
            ELSE 'n/a'
        END AS cust_marital_status,
        
        -- Standardize gender with better NULL handling
        CASE 
            WHEN cust_gender IS NULL THEN 'n/a'
            WHEN UPPER(TRIM(cust_gender)) = 'F' THEN 'Female'
            WHEN UPPER(TRIM(cust_gender)) = 'M' THEN 'Male'
            WHEN TRIM(cust_gender) = '' THEN 'n/a'
            ELSE 'n/a'
        END AS cust_gender,
        
        -- Safe date parsing with multiple format handling
        CASE 
            WHEN TRIM(cust_create_date) ~ '^\d{1,2}/\d{1,2}/\d{4}$' 
                THEN TO_DATE(TRIM(cust_create_date), 'MM/DD/YYYY')
            WHEN TRIM(cust_create_date) ~ '^\d{1,2}/\d{1,2}/\d{2}$' 
                THEN TO_DATE(TRIM(cust_create_date), 'MM/DD/YY')
            WHEN TRIM(cust_create_date) ~ '^\d{4}-\d{1,2}-\d{1,2}$' 
                THEN TO_DATE(TRIM(cust_create_date), 'YYYY-MM-DD')
            ELSE '1900-01-01'::DATE  -- Default for invalid dates
        END AS cust_create_date
    FROM deduped_customers
    WHERE rn = 1
)
SELECT 
    cust_id,
    cust_key,
    cust_firstname,
    cust_lastname,
    cust_marital_status,
    cust_gender,
    cust_create_date
FROM cleaned_data

WHERE cust_id IS NOT NULL
  AND cust_key IS NOT NULL
  AND cust_create_date != '1900-01-01'::DATE  -- Exclude records with invalid dates
  AND (cust_firstname IS NOT NULL OR cust_lastname IS NOT NULL);  -- At least one name present




-- ======================================================
--  SILVER LAYER CLEAN & TRANSFORM TABLE - CRM_PROD_INFO
-- ======================================================

-- Drop table if exists silver.prod_info

DROP TABLE IF EXISTS silver.crm_prod_info;
--DROP TABLE IF EXISTS bronze.crm_prod_info;


-- Create products table with correct name and data types
CREATE TABLE silver.crm_prod_info (
    prod_id INTEGER PRIMARY KEY,
	cat_id VARCHAR(50) UNIQUE NOT NULL,
    prod_key VARCHAR(50) UNIQUE NOT NULL,
    prod_name VARCHAR(200) NOT NULL,
    prod_cost DECIMAL(10,2),
	prod_line CHAR(20),
    prod_start_date DATE NOT NULL,
    prod_end_date DATE,
	dwh_created_at TIMESTAMP NOT NULL DEFAULT clock_timestamp()
	);

INSERT INTO silver.crm_prod_info (
    prod_id,
    cat_id,
    prod_key,
    prod_name,
    prod_cost,
    prod_line,
    prod_start_date,
    prod_end_date
	
)
SELECT DISTINCT ON (cat_id)
    prod_id,
    REPLACE(SUBSTRING(TRIM(prod_key), 1, 5), '-', '_') AS cat_id,
    SUBSTRING(prod_key, 7) AS prod_key,
    prod_name,
    COALESCE(prod_cost, 0),
    CASE 
        WHEN UPPER(TRIM(prod_line)) = 'M' THEN 'Main'
        WHEN UPPER(TRIM(prod_line)) = 'R' THEN 'Round'
		WHEN UPPER(TRIM(prod_line)) = 'S' THEN 'Other sales'
		WHEN UPPER(TRIM(prod_line)) = 'T' THEN 'Tails'
        ELSE 'n/a'
    END,
    CAST(prod_start_date AS DATE),
    CAST(prod_end_date AS DATE)
FROM bronze.crm_prod_info
ORDER BY cat_id, prod_start_date DESC;



-- ========================================================
-- SILVER LAYER CLEAN & TRANSFORM TABLE - CRM_SALES_DETAILS
-- ========================================================
-- SILVER LAYER, crm_sales _details

DROP TABLE IF EXISTS silver.crm_sales_details;

CREATE TABLE silver.crm_sales_details (
    sales_ord_num TEXT,
    sales_prod_key VARCHAR(50),
    sales_cust_id INT, 
    sales_order_date DATE,
    sales_ship_date DATE,
    sales_due_date DATE,
    sales_amount INT,
    sales_quantity INT,
    sales_price INT, 
	dwh_created_at TIMESTAMP NOT NULL DEFAULT clock_timestamp()
);


INSERT INTO silver.crm_sales_details (
    sales_ord_num,
    sales_prod_key,
    sales_cust_id, 
    sales_order_date,
    sales_ship_date,
    sales_due_date,
    sales_amount,
    sales_quantity,
    sales_price  
)
SELECT
    sales_ord_num,
    sales_prod_key,
    sales_cust_id,
    CASE
        WHEN sales_order_date = 0
          OR LENGTH(sales_order_date::TEXT) != 8
          OR sales_order_date < 19000101
          OR sales_order_date > 20500101
        THEN NULL
        ELSE TO_DATE(sales_order_date::TEXT, 'YYYYMMDD')
    END AS sales_order_date,
    CASE
        WHEN sales_ship_date = 0
          OR LENGTH(sales_ship_date::TEXT) != 8
          OR sales_ship_date < 19000101
          OR sales_ship_date > 20500101
        THEN NULL
        ELSE TO_DATE(sales_ship_date::TEXT, 'YYYYMMDD')
    END AS sales_ship_date,
    CASE
        WHEN sales_due_date = 0
          OR LENGTH(sales_due_date::TEXT) != 8  -- Fixed: was sales_ship_date
          OR sales_due_date < 19000101
          OR sales_due_date > 20500101
        THEN NULL
        ELSE TO_DATE(sales_due_date::TEXT, 'YYYYMMDD')
    END AS sales_due_date,
    CASE 
        WHEN sales_amount IS NULL 
            OR sales_amount <= 0 
            OR sales_amount != sales_quantity * ABS(sales_price)
        THEN sales_quantity * ABS(sales_price)
        ELSE sales_amount
    END AS sales_amount,  -- Changed from new_sales_amount
    sales_quantity,
    CASE 
        WHEN sales_price IS NULL 
            OR sales_price <= 0
        THEN 
            CASE 
                WHEN sales_quantity > 0 
                    AND sales_amount > 0
                THEN sales_amount / NULLIF(sales_quantity, 0)
                ELSE 0  -- Changed from NULL
            END
        ELSE ABS(sales_price)
    END AS sales_price  -- This is the corrected sales_price
FROM bronze.crm_sales_details;


-- =================================================
-- SILVER LAYER CLEAN & TRANSFORM TABLE ERP_CUST_AZ1
-- =================================================

-- Drop table if exists silver.erp_cust_az1
DROP TABLE IF EXISTS silver.erp_cust_az1;

-- Create table with appropriate data types
CREATE TABLE silver.erp_cust_az1 (
    cid VARCHAR(20) PRIMARY KEY,
    birth_date DATE,
    gender VARCHAR(10) CHECK (gender IN ('Male', 'Female')),
	dwh_created_at TIMESTAMP NOT NULL DEFAULT clock_timestamp()
);


INSERT INTO silver.erp_cust_az1 (
    cid,
    birth_date,
    gender
)
SELECT DISTINCT
    CASE 
        WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4) -- Remove Invalid Value
        ELSE cid
    END AS cid,
    bd AS birth_date,
    CASE 
        WHEN new_gender IN ('Male', 'Female') THEN new_gender
        ELSE 'Female'  -- default value to satisfy constraint
    END AS gender
FROM (
    SELECT
        cid,
        TO_DATE(birth_date, 'MM/DD/YY') AS bd,
        CASE 
            WHEN UPPER(TRIM(gender)) IN ('F', 'FEMALE') THEN 'Female' -- Data Normalization
            WHEN UPPER(TRIM(gender)) IN ('M', 'MALE') THEN 'Male'
        END AS new_gender
    FROM bronze.erp_cust_az1
    WHERE birth_date ~ '^\d{1,2}/\d{1,2}/\d{2}$' -- Conver the birth_date to DATe Format
) t
WHERE bd BETWEEN DATE '1924-01-01' AND CURRENT_DATE;


-- =========================================================
--  SILVER LAYER CLEAN & TRANSFORM PROD TABLE - ERP_LOC_A101
-- =========================================================

-- Drop table if exists silver.erp_loc_a101
DROP TABLE IF EXISTS silver.erp_loc_a101;

-- Create table with appropriate data types
CREATE TABLE silver.erp_loc_a101 (
    cid VARCHAR(20) PRIMARY KEY,
    country VARCHAR(100) NOT NULL,
	dwh_created_at TIMESTAMP NOT NULL DEFAULT clock_timestamp()
);


INSERT INTO silver.erp_loc_a101 (
    cid,
    country  
)


SELECT 
REPLACE(cid, '-','') cid,
CASE
        WHEN TRIM(country) = 'DE' THEN 'Germany'
        WHEN TRIM(country) IN ('US', 'USA') THEN 'United States'
        WHEN TRIM(country) = '' OR country IS NULL THEN 'n/a'
        ELSE TRIM(country)
    END AS country -- Normalize and handling missing value
FROM bronze.erp_loc_a101;

-- ============================================================
--  SILVER LAYER CLEAN & TRANSFORM PROD TABLE - ERP_PX_CAT_G1V2
-- ============================================================


-- Drop table if exists silver.erp_px_cat_g1v2
DROP TABLE IF EXISTS silver.erp_px_cat_g1v2;

-- Create table with appropriate data types
CREATE TABLE silver.erp_px_cat_g1v2 (
    id VARCHAR(10) PRIMARY KEY,
    category VARCHAR(50) NOT NULL,
    sub_category VARCHAR(100) NOT NULL,
    maintenance VARCHAR(3),
	dwh_created_at TIMESTAMP NOT NULL DEFAULT clock_timestamp()
);


INSERT INTO silver.erp_px_cat_g1v2 (
    id,
    category,
    sub_category,
    maintenance 
	
)

SELECT 
    id,
    category,
    sub_category,
    maintenance
FROM bronze.erp_px_cat_g1v2;

-- Check for unwanted spaces

SELECT *
FROM bronze.erp_px_cat_g1v2
WHERE category != TRIM(category) OR sub_category != TRIM(sub_category) 
OR maintenance != TRIM(maintenance);

-- Data Standardization and Consitency
--SELECT DISTINCT maintenance
--FROM bronze.erp_px_cat_g1v2

--SELECT * FROM silver.erp_px_cat_g1V2;
--SELECT * FROM silver.crm_sales_details;
