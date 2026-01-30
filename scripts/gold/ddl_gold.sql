/*
=================================================================================================================
                                      DDL Script: Create Gold Views
================================================================================================================
Scripts Purpose:
    This Scripts Create views for the "Gold Layer" in the date warehose .
    Meanwhile the Godl Layer represent the final dimention & Fact Table (Using Star Schema)
    And each views performs Transformation & combine data from "Silver Layer" to produce a clean, enriched  and
    business ready_dataset.
Usgae:
    However, these views can be queried directly for Analytics & Reporting purposes.
=================================================================================================================


-- ============================================================================================================
                              -- Creating Dimension: gold dim_customer
-- ============================================================================================================

DROP VIEW IF EXISTS gold.dim_customer;

CREATE VIEW gold.dim_customer AS
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


-- ============================================================================================================
                              -- Creating Dimension: gold dim_product
-- ============================================================================================================

DROP VIEW IF EXISTS gold.dim_product;
-- Create view gold dimension
CREATE VIEW gold.dim_product AS
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



-=========================================================================================
                    -- Creating gold.fact_sales 
-- =======================================================================================


-- Drop the view if it already exists
DROP VIEW IF EXISTS gold.fact_sales;

-- Create the view
CREATE VIEW gold.fact_sales AS
SELECT 
    ROW_NUMBER() OVER (
        ORDER BY sd.sales_ord_num
    ) AS prod_key,                     --  generate sorogate keys fron product table serial number AS prod_key

    sd.sales_ord_num      AS order_number,
    cu.customer_key,
    sd.sales_order_date   AS order_date,
    sd.sales_ship_date    AS shipping_date,
    sd.sales_due_date     AS due_date,
    sd.sales_amount,
    sd.sales_quantity,
    sd.sales_price        AS price
FROM silver.crm_sales_details sd
LEFT JOIN gold.dim_product pr
    ON sd.sales_prod_key = pr.product_number
LEFT JOIN gold.dim_customer cu
    ON sd.sales_cust_id = cu.customer_id;



