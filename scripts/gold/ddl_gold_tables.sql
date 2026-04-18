/*
===============================================================================
Script: Create Gold Layer Tables and Load Business-Ready Data
===============================================================================
Script Purpose:
    This script builds the Gold layer of the data warehouse by creating and 
    populating dimension and fact tables.

    The script performs the following actions:
    - Drops existing Gold tables if they already exist.
    - Creates dimension tables for customers and products.
    - Creates a fact table for sales transactions.
    - Loads transformed and business-ready data from the Silver layer.
    - Creates indexes to improve query performance on key columns.

Tables Created:
    - gold.dim_customers
    - gold.dim_products
    - gold.fact_sales

Usage Notes:
    - Run this script after the Silver layer has been successfully loaded.
    - This script is intended for table-based Gold layer implementation.
===============================================================================
*/


/*
===============================================================================
Create Dimension Table: gold.dim_customers
===============================================================================
Purpose:
    Stores customer-related descriptive attributes used for reporting and 
    analytical queries.

Notes:
    - customer_key is a surrogate key generated in the Gold layer.
    - Data is sourced from CRM and ERP customer/location tables in the Silver layer.
===============================================================================
*/
IF OBJECT_ID('gold.dim_customers', 'U') IS NOT NULL
    DROP TABLE gold.dim_customers;
GO

CREATE TABLE gold.dim_customers (
    customer_key      INT IDENTITY(1,1) PRIMARY KEY,
    customer_id       INT,
    customer_number   NVARCHAR(50),
    first_name        NVARCHAR(50),
    last_name         NVARCHAR(50),
    country           NVARCHAR(50),
    marital_status    NVARCHAR(50),
    gender            NVARCHAR(50),
    birthdate         DATE,
    create_date       DATE
);
GO


INSERT INTO gold.dim_customers (
    customer_id,
    customer_number,
    first_name,
    last_name,
    country,
    marital_status,
    gender,
    birthdate,
    create_date
)
SELECT
    ci.cst_id,
    ci.cst_key,
    ci.cst_firstname,
    ci.cst_lastname,
    la.cntry,
    ci.cst_marital_status,
    CASE 
        WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr 
        ELSE COALESCE(ca.gen, 'n/a')
    END AS gender,
    ca.bdate,
    ci.cst_create_date
FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ca
    ON ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 la -- Loading customer data by combining information from
    ON ci.cst_key = la.cid;      -- CRM and ERP sources
GO


/*
===============================================================================
Create Dimension Table: gold.dim_products
===============================================================================
Purpose:
    Stores product-related descriptive attributes used for reporting and 
    analytical queries.

Notes:
    - product_key is a surrogate key generated in the Gold layer.
    - Only current/active products are loaded into this dimension.
===============================================================================
*/
IF OBJECT_ID('gold.dim_products', 'U') IS NOT NULL
    DROP TABLE gold.dim_products;
GO

CREATE TABLE gold.dim_products (
    product_key      INT IDENTITY(1,1) PRIMARY KEY,
    product_id       INT,
    product_number   NVARCHAR(50),
    product_name     NVARCHAR(50),
    category_id      NVARCHAR(50),
    category         NVARCHAR(50),
    subcategory      NVARCHAR(50),
    maintenance      NVARCHAR(50),
    cost             INT,
    product_line     NVARCHAR(50),
    start_date       DATETIME
);
GO


INSERT INTO gold.dim_products (
    product_id,
    product_number,
    product_name,
    category_id,
    category,
    subcategory,
    maintenance,
    cost,
    product_line,
    start_date
)
SELECT
    pn.prd_id,
    pn.prd_key,
    pn.prd_nm,
    pn.cat_id,
    pc.cat,
    pc.subcat,
    pc.maintenance,
    pn.prd_cost,
    pn.prd_line,
    pn.prd_start_dt
FROM silver.crm_prd_info pn
LEFT JOIN silver.erp_px_cat_g1v2 pc
    ON pn.cat_id = pc.id
WHERE pn.prd_end_dt IS NULL; -- not ended
GO


/*
===============================================================================
Create Fact Table: gold.fact_sales
===============================================================================
Purpose:
    Stores transactional sales data for analytical reporting.

Notes:
    - Links to customer and product dimensions through surrogate keys.
    - Contains measures such as sales amount, quantity, and price.
===============================================================================
*/
IF OBJECT_ID('gold.fact_sales', 'U') IS NOT NULL
    DROP TABLE gold.fact_sales;
GO

CREATE TABLE gold.fact_sales (
    order_number    NVARCHAR(50),
    product_key     INT,
    customer_key    INT,
    order_date      DATE,
    shipping_date   DATE,
    due_date        DATE,
    sales_amount    INT,
    quantity        INT,
    price           INT
);
GO


INSERT INTO gold.fact_sales (
    order_number,
    product_key,
    customer_key,
    order_date,
    shipping_date,
    due_date,
    sales_amount,
    quantity,
    price
)
SELECT
    sd.sls_ord_num,
    pr.product_key,
    cu.customer_key,
    sd.sls_order_dt,
    sd.sls_ship_dt,
    sd.sls_due_dt,
    sd.sls_sales,
    sd.sls_quantity,
    sd.sls_price
FROM silver.crm_sales_details sd
LEFT JOIN gold.dim_products pr
    ON sd.sls_prd_key = pr.product_number
LEFT JOIN gold.dim_customers cu
    ON sd.sls_cust_id = cu.customer_id;
GO
-- NOTE that LEFT JOINs are used in this script so that unmatched  
-- records can still be loaded for review


/*
===============================================================================
Create Indexes for Performance Optimization
===============================================================================
Purpose:
    Creates indexes on key lookup, join, and filtering columns to improve query
    performance in the Gold layer.
===============================================================================
*/

IF NOT EXISTS (
    SELECT 1
    FROM sys.indexes
    WHERE name = 'IX_dim_customers_customer_id'
      AND object_id = OBJECT_ID('gold.dim_customers')
)
BEGIN
    CREATE INDEX IX_dim_customers_customer_id
    ON gold.dim_customers(customer_id);
END;
GO

IF NOT EXISTS (
    SELECT 1
    FROM sys.indexes
    WHERE name = 'IX_dim_products_product_number'
      AND object_id = OBJECT_ID('gold.dim_products')
)
BEGIN
    CREATE INDEX IX_dim_products_product_number
    ON gold.dim_products(product_number);
END;
GO

IF NOT EXISTS (
    SELECT 1
    FROM sys.indexes
    WHERE name = 'IX_fact_sales_customer_key'
      AND object_id = OBJECT_ID('gold.fact_sales')
)
BEGIN
    CREATE INDEX IX_fact_sales_customer_key
    ON gold.fact_sales(customer_key);
END;
GO

IF NOT EXISTS (
    SELECT 1
    FROM sys.indexes
    WHERE name = 'IX_fact_sales_product_key'
      AND object_id = OBJECT_ID('gold.fact_sales')
)
BEGIN
    CREATE INDEX IX_fact_sales_product_key
    ON gold.fact_sales(product_key);
END;
GO

IF NOT EXISTS (
    SELECT 1
    FROM sys.indexes
    WHERE name = 'IX_fact_sales_order_date'
      AND object_id = OBJECT_ID('gold.fact_sales')
)
BEGIN
    CREATE INDEX IX_fact_sales_order_date
    ON gold.fact_sales(order_date);
END;
GO
