-- ======================================================================================
/* Store Procedure: Load Silver Layer i.e (Bronze to Silver)
-- ===============================================================================================
Scripts Purpose:
  Howver, this store procedure perform the ETL i.e (Extract, Transform & Load) process
  to populate the "Silver" schema tables from the "bronze" Schema.
  
  Action Performed:
  Truncate: Silver Table
  Drop: Drop table if Existed
  Insert: Transform & Clean data from bronze to silver tables.
  
  Parameter:
  None:
  This store procedure does not accept any parameter or returns any value

Example:
  Call silver.load_silver_layer(); i.e Running this script will all the entire store procedure table
*/
-- ==================================================================================================


-- CALL silver.load_silver_layer();

-- =====================================================
-- CREATING SILVER PROCEDURE
-- =====================================================
CREATE OR REPLACE PROCEDURE silver.load_silver_layer()
LANGUAGE plpgsql
AS $$
DECLARE
    v_start_time TIMESTAMP;
    v_end_time   TIMESTAMP;
    v_batch_time INTERVAL;
BEGIN
    v_start_time := clock_timestamp();
    RAISE NOTICE 'SILVER LAYER load started at %', v_start_time;

    -- =====================================================
    -- 1. CRM_CUST_INFO
    -- =====================================================
    BEGIN
        RAISE NOTICE 'CRM_CUST_INFO load started at %', clock_timestamp();

        -- Fix marital status constraint
        ALTER TABLE silver.crm_cust_info
        DROP CONSTRAINT IF EXISTS crm_cust_info_cust_marital_status_check;

        ALTER TABLE silver.crm_cust_info
        ADD CONSTRAINT crm_cust_info_cust_marital_status_check
        CHECK (cust_marital_status IN ('Married', 'Single', 'n/a'));

        --  OPTION 1 FIX: ensure ETL audit columns exist
        ALTER TABLE silver.crm_cust_info
        ADD COLUMN IF NOT EXISTS load_start_time TIMESTAMP,
        ADD COLUMN IF NOT EXISTS load_end_time TIMESTAMP,
        ADD COLUMN IF NOT EXISTS batch_time INTERVAL;

        TRUNCATE TABLE silver.crm_cust_info;

        INSERT INTO silver.crm_cust_info (
            cust_id,
            cust_key,
            cust_firstname,
            cust_lastname,
            cust_marital_status,
            cust_gender,
            cust_create_date,
            load_start_time,
            load_end_time,
            batch_time
        )
        WITH deduped_customers AS (
            SELECT *,
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
            WHERE cust_id IS NOT NULL
        ),
        cleaned_data AS (
            SELECT
                cust_id,
                cust_key,
                NULLIF(TRIM(cust_firstname), '') AS cust_firstname,
                NULLIF(TRIM(cust_lastname), '') AS cust_lastname,
                CASE 
                    WHEN cust_marital_status IS NULL THEN 'n/a'
                    WHEN UPPER(TRIM(cust_marital_status)) = 'M' THEN 'Married'
                    WHEN UPPER(TRIM(cust_marital_status)) = 'S' THEN 'Single'
                    ELSE 'n/a'
                END AS cust_marital_status,
                CASE 
                    WHEN cust_gender IS NULL THEN 'n/a'
                    WHEN UPPER(TRIM(cust_gender)) = 'F' THEN 'Female'
                    WHEN UPPER(TRIM(cust_gender)) = 'M' THEN 'Male'
                    ELSE 'n/a'
                END AS cust_gender,
                CASE 
                    WHEN TRIM(cust_create_date) ~ '^\d{1,2}/\d{1,2}/\d{4}$'
                        THEN TO_DATE(TRIM(cust_create_date), 'MM/DD/YYYY')
                    WHEN TRIM(cust_create_date) ~ '^\d{1,2}/\d{1,2}/\d{2}$'
                        THEN TO_DATE(TRIM(cust_create_date), 'MM/DD/YY')
                    WHEN TRIM(cust_create_date) ~ '^\d{4}-\d{1,2}-\d{1,2}$'
                        THEN TO_DATE(TRIM(cust_create_date), 'YYYY-MM-DD')
                    ELSE '1900-01-01'::DATE
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
            cust_create_date,
            v_start_time,
            clock_timestamp(),
            clock_timestamp() - v_start_time
        FROM cleaned_data
        WHERE cust_key IS NOT NULL
          AND cust_create_date <> '1900-01-01'::DATE
          AND (cust_firstname IS NOT NULL OR cust_lastname IS NOT NULL);

        v_end_time := clock_timestamp();
        v_batch_time := v_end_time - v_start_time;
        RAISE NOTICE 'CRM_CUST_INFO load completed at % | Duration: %', v_end_time, v_batch_time;

    EXCEPTION
        WHEN OTHERS THEN
            RAISE EXCEPTION 'CRM_CUST_INFO load failed: % | SQLSTATE: %', SQLERRM, SQLSTATE;
    END;

-- =====================================================
-- 2. CRM_PROD_INFO
-- =====================================================
BEGIN
    RAISE NOTICE 'CRM_PROD_INFO load started at %', clock_timestamp();

    DROP TABLE IF EXISTS silver.crm_prod_info;

    CREATE TABLE silver.crm_prod_info (
        prod_id INTEGER PRIMARY KEY,
        cat_id VARCHAR(50) UNIQUE NOT NULL,
        prod_key VARCHAR(50) UNIQUE NOT NULL,
        prod_name VARCHAR(200) NOT NULL,
        prod_cost DECIMAL(10,2),
        prod_line CHAR(20),
        prod_start_date DATE NOT NULL,
        prod_end_date DATE,
        load_start_time TIMESTAMP,
        load_end_time TIMESTAMP,
        batch_time INTERVAL,
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
        prod_end_date,
        load_start_time,
        load_end_time,
        batch_time
    )
    SELECT DISTINCT ON (cat_id)
        prod_id,
        cat_id,
        prod_key,
        prod_name,
        prod_cost,
        prod_line,
        prod_start_date,
        prod_end_date,
        v_start_time AS load_start_time,
        clock_timestamp() AS load_end_time,
        clock_timestamp() - v_start_time AS batch_time
    FROM (
        SELECT
            prod_id,
            REPLACE(SUBSTRING(TRIM(prod_key), 1, 5), '-', '_') AS cat_id,
            SUBSTRING(prod_key, 7) AS prod_key,
            prod_name,
            COALESCE(prod_cost, 0) AS prod_cost,
            CASE 
                WHEN UPPER(TRIM(prod_line)) = 'M' THEN 'Main'
                WHEN UPPER(TRIM(prod_line)) = 'R' THEN 'Round'
                WHEN UPPER(TRIM(prod_line)) = 'S' THEN 'Other sales'
                WHEN UPPER(TRIM(prod_line)) = 'T' THEN 'Tails'
                ELSE 'n/a'
            END AS prod_line,
            CAST(prod_start_date AS DATE) AS prod_start_date,
            CAST(prod_end_date AS DATE) AS prod_end_date
        FROM bronze.crm_prod_info
    ) t
    ORDER BY cat_id, prod_start_date DESC;

    v_end_time := clock_timestamp();
    v_batch_time := v_end_time - v_start_time;
    RAISE NOTICE 'CRM_PROD_INFO load completed at % | Duration: %',
        v_end_time, v_batch_time;

EXCEPTION
    WHEN OTHERS THEN
        RAISE EXCEPTION 'CRM_PROD_INFO load failed: % | SQLSTATE: %',
            SQLERRM, SQLSTATE;
END;

     -- =====================================================
    -- 3. CRM_SALES_DETAILS
    -- =====================================================
    BEGIN
        RAISE NOTICE 'CRM_SALES_DETAILS load started at %', clock_timestamp();

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
            load_start_time TIMESTAMP,
            load_end_time TIMESTAMP,
            batch_time INTERVAL,
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
            sales_price,
            load_start_time,
            load_end_time,
            batch_time
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
            END,
            CASE
                WHEN sales_ship_date = 0
                  OR LENGTH(sales_ship_date::TEXT) != 8
                  OR sales_ship_date < 19000101
                  OR sales_ship_date > 20500101
                THEN NULL
                ELSE TO_DATE(sales_ship_date::TEXT, 'YYYYMMDD')
            END,
            CASE
                WHEN sales_due_date = 0
                  OR LENGTH(sales_due_date::TEXT) != 8
                  OR sales_due_date < 19000101
                  OR sales_due_date > 20500101
                THEN NULL
                ELSE TO_DATE(sales_due_date::TEXT, 'YYYYMMDD')
            END,
            CASE 
                WHEN sales_amount IS NULL OR sales_amount <= 0 OR sales_amount != sales_quantity * ABS(sales_price)
                THEN sales_quantity * ABS(sales_price)
                ELSE sales_amount
            END,
            sales_quantity,
            CASE 
                WHEN sales_price IS NULL OR sales_price <= 0
                THEN CASE WHEN sales_quantity > 0 AND sales_amount > 0 THEN sales_amount / NULLIF(sales_quantity,0) ELSE 0 END
                ELSE ABS(sales_price)
            END,
            v_start_time AS load_start_time,
            clock_timestamp() AS load_end_time,
            clock_timestamp() - v_start_time AS batch_time
        FROM bronze.crm_sales_details;

        v_end_time := clock_timestamp();
        v_batch_time := v_end_time - v_start_time;
        RAISE NOTICE 'CRM_SALES_DETAILS load completed at % | Duration: %', v_end_time, v_batch_time;

    EXCEPTION
        WHEN OTHERS THEN
            RAISE EXCEPTION 'CRM_SALES_DETAILS load failed: % | SQLSTATE: %', SQLERRM, SQLSTATE;
    END;

    -- =====================================================
    -- 4. ERP_CUST_AZ1
    -- =====================================================
    BEGIN
        RAISE NOTICE 'ERP_CUST_AZ1 load started at %', clock_timestamp();

        DROP TABLE IF EXISTS silver.erp_cust_az1;
        CREATE TABLE silver.erp_cust_az1 (
            cid VARCHAR(20) PRIMARY KEY,
            birth_date DATE,
            gender VARCHAR(10) CHECK (gender IN ('Male', 'Female')),
            load_start_time TIMESTAMP,
            load_end_time TIMESTAMP,
            batch_time INTERVAL,
            dwh_created_at TIMESTAMP NOT NULL DEFAULT clock_timestamp()
        );

        INSERT INTO silver.erp_cust_az1 (
            cid,
            birth_date,
            gender,
            load_start_time,
            load_end_time,
            batch_time
        )
        SELECT DISTINCT
            CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4) ELSE cid END AS cid,
            TO_DATE(birth_date, 'MM/DD/YY') AS birth_date,
            CASE 
                WHEN UPPER(TRIM(gender)) IN ('F', 'FEMALE') THEN 'Female'
                WHEN UPPER(TRIM(gender)) IN ('M', 'MALE') THEN 'Male'
                ELSE 'Female'
            END AS gender,
            v_start_time AS load_start_time,
            clock_timestamp() AS load_end_time,
            clock_timestamp() - v_start_time AS batch_time
        FROM bronze.erp_cust_az1
        WHERE birth_date ~ '^\d{1,2}/\d{1,2}/\d{2}$'
          AND TO_DATE(birth_date, 'MM/DD/YY') BETWEEN DATE '1924-01-01' AND CURRENT_DATE;

        v_end_time := clock_timestamp();
        v_batch_time := v_end_time - v_start_time;
        RAISE NOTICE 'ERP_CUST_AZ1 load completed at % | Duration: %', v_end_time, v_batch_time;

    EXCEPTION
        WHEN OTHERS THEN
            RAISE EXCEPTION 'ERP_CUST_AZ1 load failed: % | SQLSTATE: %', SQLERRM, SQLSTATE;
    END;

    -- =====================================================
    -- 5. ERP_LOC_A101
    -- =====================================================
    BEGIN
        RAISE NOTICE 'ERP_LOC_A101 load started at %', clock_timestamp();

        DROP TABLE IF EXISTS silver.erp_loc_a101;
        CREATE TABLE silver.erp_loc_a101 (
            cid VARCHAR(20) PRIMARY KEY,
            country VARCHAR(100) NOT NULL,
            load_start_time TIMESTAMP,
            load_end_time TIMESTAMP,
            batch_time INTERVAL,
            dwh_created_at TIMESTAMP NOT NULL DEFAULT clock_timestamp()
        );

        INSERT INTO silver.erp_loc_a101 (
            cid,
            country,
            load_start_time,
            load_end_time,
            batch_time
        )
        SELECT
            REPLACE(cid, '-', '') AS cid,
            CASE
                WHEN TRIM(country) = 'DE' THEN 'Germany'
                WHEN TRIM(country) IN ('US','USA') THEN 'United States'
                WHEN TRIM(country) = '' OR country IS NULL THEN 'n/a'
                ELSE TRIM(country)
            END AS country,
            v_start_time AS load_start_time,
            clock_timestamp() AS load_end_time,
            clock_timestamp() - v_start_time AS batch_time
        FROM bronze.erp_loc_a101;

        v_end_time := clock_timestamp();
        v_batch_time := v_end_time - v_start_time;
        RAISE NOTICE 'ERP_LOC_A101 load completed at % | Duration: %', v_end_time, v_batch_time;

    EXCEPTION
        WHEN OTHERS THEN
            RAISE EXCEPTION 'ERP_LOC_A101 load failed: % | SQLSTATE: %', SQLERRM, SQLSTATE;
    END;

    -- =====================================================
    -- 6. ERP_PX_CAT_G1V2
    -- =====================================================
    BEGIN
        RAISE NOTICE 'ERP_PX_CAT_G1V2 load started at %', clock_timestamp();

        DROP TABLE IF EXISTS silver.erp_px_cat_g1v2;
        CREATE TABLE silver.erp_px_cat_g1v2 (
            id VARCHAR(10) PRIMARY KEY,
            category VARCHAR(50) NOT NULL,
            sub_category VARCHAR(100) NOT NULL,
            maintenance VARCHAR(3),
            load_start_time TIMESTAMP,
            load_end_time TIMESTAMP,
            batch_time INTERVAL,
            dwh_created_at TIMESTAMP NOT NULL DEFAULT clock_timestamp()
        );

        INSERT INTO silver.erp_px_cat_g1v2 (
            id,
            category,
            sub_category,
            maintenance,
            load_start_time,
            load_end_time,
            batch_time
        )
        SELECT
            id,
            category,
            sub_category,
            maintenance,
            v_start_time AS load_start_time,
            clock_timestamp() AS load_end_time,
            clock_timestamp() - v_start_time AS batch_time
        FROM bronze.erp_px_cat_g1v2;

        v_end_time := clock_timestamp();
        v_batch_time := v_end_time - v_start_time;
        RAISE NOTICE 'ERP_PX_CAT_G1V2 load completed at % | Duration: %', v_end_time, v_batch_time;

    EXCEPTION
        WHEN OTHERS THEN
            RAISE EXCEPTION 'ERP_PX_CAT_G1V2 load failed: % | SQLSTATE: %', SQLERRM, SQLSTATE;
    END;

    -- =====================================================
    -- SILVER LAYER COMPLETED
    -- =====================================================
    v_end_time := clock_timestamp();
    v_batch_time := v_end_time - v_start_time;
    RAISE NOTICE 'SILVER LAYER load completed successfully';
    RAISE NOTICE 'Total start: %, Total end: %, Total duration: %', v_start_time, v_end_time, v_batch_time;

END;
$$;




