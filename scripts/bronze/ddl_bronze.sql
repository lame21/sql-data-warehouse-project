-- ===============================================================
-- Script: Create Bronze Layer Tables
-- Description:
--   This script creates the bronze layer tables for staging 
--   data in the Data Warehouse. Each CREATE statement includes
--   a safeguard DROP statement to remove the table if it 
--   already exists, ensuring clean re-creation.
-- Author: Lame Motshabi
-- Run this script to redefine the DDL structure of bronze tables
-- ===============================================================

-- ===========================================================
-- Customer Information Table (CRM System)
-- Stores basic customer details such as name, marital status,
-- gender, and creation date.
-- ===========================================================
DROP TABLE IF EXISTS bronze.crm_cust_info;
CREATE TABLE bronze.crm_cust_info(
    cst_id INT,                      -- Customer ID
    cst_key VARCHAR(50),             -- Unique Customer Key
    cst_firstname VARCHAR(50),       -- First Name
    cst_lastname VARCHAR(50),        -- Last Name
    cst_marital_status VARCHAR(50),  -- Marital Status
    cst_gndr VARCHAR(50),            -- Gender
    cst_create_date DATE             -- Customer creation date
);

-- ===========================================================
-- Product Information Table (CRM System)
-- Contains details about products including cost, category 
-- line, and active dates.
-- ===========================================================
DROP TABLE IF EXISTS bronze.crm_prd_info;
CREATE TABLE bronze.crm_prd_info(
    prd_id INT,                      -- Product ID
    prd_key VARCHAR(50),             -- Product Key
    prd_nm VARCHAR(50),              -- Product Name
    prd_cost INT,                    -- Product Cost
    prd_line VARCHAR(50),            -- Product Line (category)
    prd_start_dt DATE,               -- Product availability start date
    prd_end_dt DATE                  -- Product availability end date
);

-- ===========================================================
-- Sales Details Table (CRM System)
-- Tracks customer sales transactions, including product, 
-- order dates, and pricing information.
-- ===========================================================
DROP TABLE IF EXISTS bronze.crm_sales_details;
CREATE TABLE bronze.crm_sales_details(
    sls_ord_num VARCHAR(50),         -- Sales Order Number
    sls_prd_key VARCHAR(50),         -- Product Key (FK to crm_prd_info)
    sls_cust_id INT,                 -- Customer ID (FK to crm_cust_info)
    sls_order_dt DATE,               -- Order Date
    sls_ship_dt DATE,                -- Shipment Date
    sls_due_dt DATE,                 -- Payment Due Date
    sls_sales INT,                   -- Total Sales Amount
    sls_quantity INT,                -- Quantity Sold
    sls_price INT                    -- Price per Unit
);

-- ===========================================================
-- Location Table (ERP System)
-- Stores country-level location data for ERP customers.
-- ===========================================================
DROP TABLE IF EXISTS bronze.erp_loc_a101;
CREATE TABLE bronze.erp_loc_a101(
    cid VARCHAR(50),                 -- Customer ID
    cntry VARCHAR(50)                -- Country
);

-- ===========================================================
-- Customer Demographics Table (ERP System)
-- Holds basic demographic data such as birth date and gender.
-- ===========================================================
DROP TABLE IF EXISTS bronze.erp_cust_az12;
CREATE TABLE bronze.erp_cust_az12(
    cid VARCHAR(50),                 -- Customer ID
    bdate DATE,                      -- Birth Date
    gen VARCHAR(50)                  -- Gender
);

-- ===========================================================
-- Product Category Table (ERP System)
-- Captures product categorization and maintenance details.
-- ===========================================================
DROP TABLE IF EXISTS bronze.erp_px_cat_g1v2;
CREATE TABLE bronze.erp_px_cat_g1v2(
    id  VARCHAR(50),                 -- Product ID
    cat VARCHAR(50),                 -- Product Category
    subcat VARCHAR(50),              -- Product Sub-category
    maintenance VARCHAR(50)          -- Maintenance details
);
