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

-- Drop table if exists bronze.crm_sales_details

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

-- Drop table if exists bronze.erp_cust_az1
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

-- Drop table if exists bronze.erp_loc_a101
DROP TABLE IF EXISTS bronze.erp_loc_a101;

-- Create table with appropriate data types
CREATE TABLE bronze.erp_loc_a101 (
    cid VARCHAR(20) PRIMARY KEY,
    country VARCHAR(100) NOT NULL
);

-- =============================================
--  BRONZE LAYER RAW PROD TABLE - ERP_PX_CAT_G1V2
-- =============================================

-- Drop table if exists bronze.erp_px_cat_g1v2
DROP TABLE IF EXISTS bronze.erp_px_cat_g1v2;

-- Create table with appropriate data types
CREATE TABLE bronze.erp_px_cat_g1v2 (
    id VARCHAR(10) PRIMARY KEY,
    category VARCHAR(50) NOT NULL,
    sub_category VARCHAR(100) NOT NULL,
    maintenance VARCHAR(3)
);




-- ====================================================================================================
## Upload the CSV File to Postgres sql from using python VScode to insert data into the table database
-- ====================================================================================================

-- Create a virtual Environment.
-- Install the require library
-- Open the Complete data warehouse pipeline project file where we have source crm and erp.
-- Then connect the postgress server engine from vitual studio vsCode in other to insert date into schema.


-- ====================================================================================	
 -- ## Let import require library to read our dataset and connect to SQL postgress engine
-- ====================================================================================
	
import pandas as pd
import psycopg2
from sqlalchemy import create_engine, text
	
-- =============================================================================================
-- #### Let profiling by given the username and password our data by connecting postgres sql server
-- =============================================================================================	

USERNAME ="postgres"
PASSWORD ="password"
HOST ="localhost"
PORT ="5432"
DB_NAME ="DataWarehouse"
	

engine = create_engine(f"postgresql+psycopg2://{USERNAME}):{PASSWORD}@{HOST}:{PORT}/{DB_NAME}")
	
-- ======================================
-- ## Let create the engine from Sqlalchemy
-- ======================================	

from sqlalchemy import create_engine

engine = create_engine(
    "postgresql+psycopg2://postgres:password@localhost:5432/DataWarehouse"
)


-- ======================================================================	
-- ## let see the first postgres server maybe it has coonect successfully
-- ======================================================================
	
from sqlalchemy import text

with engine.connect() as conn:
    result = conn.execute(text("SELECT version();"))
    print(result.fetchone())

-- ==============================
#### A. bronze.crm_cust_info
-- =============================	

-- ===============================================	
-- ## Let see & read the Pandas dataframe of dataset
-- ===============================================

df = pd.read_csv('/Users/user/Desktop/data_wareHouse_pipeline_project /source_crm/cust_info.csv')
	
-- =======================================
-- ## Let see the first 5Row of our dataset
-- ======================================

df.head(5)
	
-- ===================================================
-- ## Let connect the dataset to postgres SQl database
-- ==================================================	

df.to_sql(
    name="crm_cust_info",
    con=engine,
    schema="bronze",
    if_exists="replace",
    index=False
)

-- ==================================================================================================
-- ## Let test the connection server to our postgress data output was successful from our python code 
-- ================================================================================================== 

with engine.connect() as conn:
    count = conn.execute(
        text("SELECT COUNT(*) FROM bronze.crm_cust_info;")
    ).scalar()

    print("Rows in postgres:", count)

    sample = conn.execute(
        text("SELECT * FROM bronze.crm_cust_info LIMIT 5;")
    ).fetchall()

    print("Sample data:", sample)


-- ==========================	
### B. bronze.crm_prod_info
-- ==========================

df = pd.read_csv('/Users/user/Desktop/data_wareHouse_pipeline_project /source_crm/prod_info.csv')
	
-- ========================================
-- ## Let see the first 5Row of our dataset
-- =======================================	

df.head(5)

-- ====================================================	
-- ## Let connect the dataset to postgres SQl database
-- ====================================================
	
df.to_sql(
    name="crm_prod_info",
    con=engine,
    schema="bronze",
    if_exists="replace",
    index=False
)


-- =============================================================================================	
-- ## Let test the connection server to our postgress data output was successful in postgres sql
-- =============================================================================================
	
with engine.connect() as conn:
    count = conn.execute(
        text("SELECT COUNT(*) FROM bronze.crm_prod_info;")
    ).scalar()

    print("Rows in postgres:", count)

    sample = conn.execute(
        text("SELECT * FROM bronze.crm_prod_info LIMIT 5;")
    ).fetchall()

    print("Sample data:", sample)

-- ================================
### C. bronze.crm_sales_details
-- ================================	

-- =================================================	
-- ## Let see & read the Pandas dataframe of dataset
-- =================================================	

df = pd.read_csv('/Users/user/Desktop/data_wareHouse_pipeline_project /source_crm/sales_details.csv')

-- =======================================	
-- ## Let see the first 5Row of our dataset
-- ======================================

df.head(5)

-- =======================================================
-- ##  Let connect the dataset to postgres SQl server database
-- ========================================================
	
df.to_sql(
    name="crm_sales_details",
    con=engine,
    schema="bronze",
    if_exists="replace",
    index=False
)

-- ============================================================================================
-- ## Let test the connection server to our postgress data output was successful in postgres sql
-- ===========================================================================================-	

with engine.connect() as conn:
    count = conn.execute(
        text("SELECT COUNT(*) FROM bronze.crm_sales_details;")
    ).scalar()

    print("Rows in postgres:", count)

    sample = conn.execute(
        text("SELECT * FROM bronze.crm_sales_details LIMIT 5;")
    ).fetchall()

    print("Sample data:", sample)

-- ==========================
### D. bronze.erp_cust_az1
-- ===========================
	
-- ================================================
-- ## Let see & read the Pandas dataframe of dataset
-- =================================================	

df = pd.read_csv('/Users/user/Desktop/data_wareHouse_pipeline_project /source_erp/cust_az1.csv')

-- =========================================
-- ## Let see the first 5Row of our dataset
-- ==========================================
	
df.head(5)

-- ==================================================
-- ## Let connect the dataset to postgres SQl database
-- ==================================================
	
df.to_sql(
    name="erp_cust_az1",
    con=engine,
    schema="bronze",
    if_exists="replace",
    index=False
)

-- =================================================================================================================================
-- ## Let test the connection server to our postgress inside the vscode output ipynb file data output was successful in postgres sql
-- =================================================================================================================================
	
with engine.connect() as conn:
    count = conn.execute(
        text("SELECT COUNT(*) FROM bronze.erp_cust_az1;")
    ).scalar()

    print("Rows in postgres:", count)

    sample = conn.execute(
        text("SELECT * FROM bronze.erp_cust_az1 LIMIT 5;")
    ).fetchall()

    print("Sample data:", sample)


--============================
### E. bronze.erp_loc_a101
-- ============================
	
-- =================================================
-- ## Let see & read the Pandas dataframe of dataset
-- =================================================
	
df = pd.read_csv('/Users/user/Desktop/data_wareHouse_pipeline_project /source_erp/loc_a101.csv')

-- ========================================	
-- # Let see the first 5Row of our dataset
-- ========================================
	
df.head(5)

-- ===================================================	
-- ## Let connect the dataset to postgres SQl database
-- ===================================================
	
df.to_sql(
    name="erp_loc_a101",
    con=engine,
    schema="bronze",
    if_exists="replace",
    index=False
)

-- ===================================================================================================================
-- ## Let test the connection server to our postgress data output was successful in postgres sql fron our python output
-- ===================================================================================================================

with engine.connect() as conn:
    count = conn.execute(
        text("SELECT COUNT(*) FROM bronze.erp_loc_a101;")
    ).scalar()

    print("Rows in postgres:", count)

    sample = conn.execute(
        text("SELECT * FROM bronze.erp_loc_a101 LIMIT 5;")
    ).fetchall()

    print("Sample data:", sample)

-- ===========================
### F. bronze.erp_px_cat_g1v2
-- ===========================

-- ==================================================
-- ## Let see & read the Pandas dataframe of dataset
-- ==================================================

df = pd.read_csv('/Users/user/Desktop/data_wareHouse_pipeline_project /source_erp/px_cat_g1v2.csv')
	
-- ========================================
-- ## Let see the first 5Row of our dataset
-- ========================================
	
df.head(5)

-- ====================================================	
-- ## Let connect the dataset to postgres SQl database
-- ====================================================

df.to_sql(
    name="erp_cat_giv2",
    con=engine,
    schema="bronze",
    if_exists="replace",
    index=False
)

-- ==============================================================================================
-- ## Let test the connection server to our postgress data output was successful in postgres sql
-- ==============================================================================================

with engine.connect() as conn:
    count = conn.execute(
        text("SELECT COUNT(*) FROM bronze.erp_loc_a101;")
    ).scalar()

    print("Rows in postgres:", count)

    sample = conn.execute(
        text("SELECT * FROM bronze.erp_loc_a101 LIMIT 5;")
    ).fetchall()

    print("Sample data:", sample)







