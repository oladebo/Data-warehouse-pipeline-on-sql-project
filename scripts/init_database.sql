-- ===============================================
/* Create Database and schemas
-- ===============================================
Scripts Purpose:
  This scripts create a new database name "DataWarehose" after checking if is already exist, If the databese still exist,
  it is going to be drop and recreated. However the scripts sets three schemas within database "bronze", "silver", and "gold"

Warning:
  Running this script will drop will drop the entire "DataWarehose" Database if it's exists, All databes will be permanently 
  deleted. proceed with caution and ensure you have proper back_up before running the script
*/

  
  
  
-- =============================================
-- Run this as superuser (postgres)
DROP DATABASE IF EXISTS DataWarehouse;
CREATE DATABASE DataWarehouse
    WITH 
    OWNER = postgres
    ENCODING = 'UTF8'
    LC_COLLATE = 'en_US.UTF-8'
    LC_CTYPE = 'en_US.UTF-8'
    TABLESPACE = pg_default
    CONNECTION LIMIT = -1
    IS_TEMPLATE = False;

-- COMMENT ON DATABASE master_data_warehouse IS 'DataWarehouse with Bronze-Silver-Gold Architecture';



  
-- =============================================
--  BRONZE LAYER RAW CUST TABLE - CRM_CUST_INFO
-- =============================================


-- Drop table if exists bronze.crm_cust_info

DROP TABLE IF EXISTS bronze.crm_cust_info;


CREATE TABLE bronze.crm_cust_info (
    cust_id INTEGER PRIMARY KEY,
    cust_key VARCHAR(20) UNIQUE NOT NULL,
    cust_firstname VARCHAR(50) NOT NULL,
    cust_lastname VARCHAR(50) NOT NULL,
    cust_marital_status CHAR(20) CHECK (cust_marital_status IN ('M', 'S', 'D', 'W')),
    cust_gender CHAR(10) CHECK (cust_gender IN ('M', 'F')),
    cust_create_date DATE NOT NULL
	);
			
-- =============================================
--  BRONZE LAYER RAW PROD TABLE - CRM_PROD_INFO
-- =============================================


-- Drop table if exists bronze.prod_info
DROP TABLE IF EXISTS bronze.crm_prod_info;

-- Create products table with correct name and data types
CREATE TABLE bronze.crm_prod_info (
    prod_id INTEGER PRIMARY KEY,
    prod_key VARCHAR(50) UNIQUE NOT NULL,
    prod_name VARCHAR(200) NOT NULL,
    prod_cost DECIMAL(10,2),
    prod_line CHAR(1) CHECK (prod_line IN ('R', 'S')),
    prod_start_date DATE NOT NULL,
    prod_end_date DATE
	);
	
	
-- =============================================
-- BRONZE LAYER RAW SALES TABLE - CRM_SALES_DETAILS
-- =============================================


DROP TABLE IF EXISTS bronze.crm_sales_details;

CREATE TABLE bronze.crm_sales_details (
    sales_ord_num VARCHAR(20),
    sales_prod_key VARCHAR(50),
    sales_cust_id VARCHAR(20), 
    sales_order_date VARCHAR(20),
    sales_ship_date VARCHAR(20),
    sales_due_date VARCHAR(20),
    sales_amount VARCHAR(20),
    sales_quantity VARCHAR(20),
    sales_price VARCHAR(20)  
);


-- =============================================
-- BRONZE LAYER RAW ERP_CUST_AZ1
-- =============================================

-- Drop table if exists
DROP TABLE IF EXISTS bronze.erp_cust_az1;

-- Create table with appropriate data types
CREATE TABLE bronze.erp_cust_az1 (
    cid VARCHAR(20) PRIMARY KEY,
    birth_date DATE NOT NULL,
    gender VARCHAR(10) CHECK (gender IN ('Male', 'Female', 'Other'))
);


-- =============================================
--  BRONZE LAYER RAW PROD TABLE - ERP_LOC_A101
-- =============================================

-- Drop table if exists
DROP TABLE IF EXISTS bronze.erp_loc_a101;

-- Create table with appropriate data types
CREATE TABLE bronze.erp_loc_a101 (
    cid VARCHAR(20) PRIMARY KEY,
    country VARCHAR(100) NOT NULL
);

-- =============================================
--  BRONZE LAYER RAW PROD TABLE - ERP_LOC_A101
-- =============================================

-- Drop table if exists
DROP TABLE IF EXISTS bronze.erp_loc_a101;

-- Create table with appropriate data types
CREATE TABLE bronze.erp_loc_a101 (
    id VARCHAR(10) PRIMARY KEY,
    category VARCHAR(50) NOT NULL,
    sub_category VARCHAR(100) NOT NULL,
    maintenance VARCHAR(3)
);






CREATE SCHEMA IF NOT EXISTS bronze;    -- Raw/landing zone
CREATE SCHEMA IF NOT EXISTS silver;    -- Cleaned/transformed data
CREATE SCHEMA IF NOT EXISTS gold;      -- Business-ready/aggregated data
