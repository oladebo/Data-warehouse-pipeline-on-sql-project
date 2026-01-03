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







CREATE SCHEMA IF NOT EXISTS bronze;    -- Raw/landing zone
CREATE SCHEMA IF NOT EXISTS silver;    -- Cleaned/transformed data
CREATE SCHEMA IF NOT EXISTS gold;      -- Business-ready/aggregated data
