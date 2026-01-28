## Data Dictionary for Gold Layer

### Overview
Gold Layer (Business / Consumption Layer)
The Gold layer is the final, refined layer of the data pipeline. It contains clean, trusted, and business-ready data designed
for analytics, reporting, and decision-making.


1. gold.dim_customer
gold.dim_customer is a customer dimension table in the Gold layer. Its main purpose is to provide a single, trusted, 
business-ready view of each customer for analytics and reporting.

| gold.dim_customer |                |                                                                                        |
|-------------------|----------------|----------------------------------------------------------------------------------------|
|                   |                |                                                                                        |
| customer_key      | BIGINT         | Sorogate key unique identified each customer record in the dimention table             |
| customer_id       | INTEGER        | unique numerical identifier assigned to each customer                                  |
| customer_number   | CHARACTER(20)  | Aipha-numerical Identifier representing the customer used for tracking and referencing |
| first_name        | CHARACTER(50)  | The customer's first name as recorded in the system                                    |
| last_name         | CHARACTER(50)  | The customer last name or family name                                                  |
| marital_status    | CHARACTER(20)  | The marital status of the customer e.g Male or Femal                                   |
| gender_           | CHARACTER(100) | The Customer gender either Single or Married                                           |
| country           | CHARACTER(100) | The customer Contry location                                                           |
| birthdate         | DATE           | The cistomer's date of birth                                                           |
| create_date       | DATE           | The day customer's  transaction was created                                            |



2. gold.dim_fact_sales
gold.fact_sales stores measurable business events related to sales.Its job is to answer “what happened, when, and how much?”

| gold.fact_sales |            |                                                                                          |
|-----------------|------------|------------------------------------------------------------------------------------------|
|                 |            |                                                                                          |
| Column Name     | Data Type  | Description                                                                              |
| Order_number    | TEXT       | Aipha-numerical Identifier representing the order sale used for tracking and referencing |
| product_key     | BIGINT     | Sorogate key unique identified each product record in the dimention table                |
| customer_key    | BIGINT     | Sorogate key unique identified each customer record in the dimention table               |
| order_date      | DATE       | The date order was initated                                                              |
| shipping_date   | DATE       | The date Order product coming                                                            |
| due_date        | DATE       | The Order due date                                                                       |
| sales_amount    | INT        | The sales amount of each product sold                                                    |
| sales_quantity  | INT        | The quantity order made for each day                                                     |
| price           | INTEGER    | The price of each product                                                                |



3. gold.dim_product
gold.dim_product is a product dimension table in the Gold layer.
Its purpose is to provide a single, consistent, analytics-ready view of each product used across all reports and dashboards.


| gold.dim_product |                |                                                                                       |
|-------------------|----------------|---------------------------------------------------------------------------------------|
|                   |                |                                                                                       |
| product_key       | BIGINT         | Sorogate key unique identified each product's record in the dimention table           |
| product_id        | INTEGER        | Unique numerical identifier assigned to each product                                  |
| product_number    | CHARACTER(50)  | Aipha-numerical Identifier representing the product used for tracking and referencing |
| product_name      | CHARACTER(200) | The product's name as recorded in the system                                          |
| category_id       | CHARACTER(50)  | Special Unique character identifier assigned to each product category                 |
| category          | CHARACTER(50)  | Aipha-numerical Identifier representing the product used for tracking and referencing |
| subcategory       | CHARACTER(100) | Subcategory identifier assigned to each product category                              |
| maintenance       | CHARACTER(3)   | Unique numerical identifier assigned to each product category                         |
| product_cost      | NUMERIC(10.2)  | The product unit cost of each product                                                 |
| product_line      | CHARACTER(20)  | Special unique character identifier assigned to each product                          |
| start_date        | DATE           | The original date of product produce                                                  |









