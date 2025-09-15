-- ===========================================================
-- Script: Load Data into Bronze Schema Tables
-- Description:
--   This script loads raw CSV files into staging (bronze) 
--   layer tables using the \copy command. 
--   The \copy command runs from the client (psql), so it 
--   works with files on your local machine.
-- Usage:
--   Run from terminal (PowerShell / cmd / bash):
--   psql -U <username> -d DataWarehouse -f load_bronze_data.sql
-- ===========================================================

-- ===========================================================
-- Load Customer Info Data (CRM)
-- CSV contains: cst_id, cst_key, cst_firstname, cst_lastname,
--               cst_marital_status, cst_gndr, cst_create_date
-- ===========================================================
\copy bronze.crm_cust_info
FROM 'filepath'
WITH (FORMAT csv, HEADER true, DELIMITER ',', ENCODING 'UTF8');

-- ===========================================================
-- Load Product Info Data (CRM)
-- CSV contains: prd_id, prd_key, prd_nm, prd_cost, prd_line,
--               prd_start_dt, prd_end_dt
-- ===========================================================
\copy bronze.crm_prd_info
FROM 'filepath'
WITH (FORMAT csv, HEADER true, DELIMITER ',', ENCODING 'UTF8');

-- ===========================================================
-- Load Sales Details Data (CRM)
-- CSV contains: sls_ord_num, sls_prd_key, sls_cust_id, 
--               sls_order_dt, sls_ship_dt, sls_due_dt, 
--               sls_sales, sls_quantity, sls_price
-- ===========================================================
\copy bronze.crm_sales_details
FROM 'filepath'
WITH (FORMAT csv, HEADER true, DELIMITER ',', ENCODING 'UTF8');

-- ===========================================================
-- Load Location Data (ERP)
-- CSV contains: cid, cntry
-- ===========================================================
\copy bronze.erp_loc_a101
FROM 'filepath'
WITH (FORMAT csv, HEADER true, DELIMITER ',', ENCODING 'UTF8');

-- ===========================================================
-- Load Customer Demographics Data (ERP)
-- CSV contains: cid, bdate, gen
-- ===========================================================
\copy bronze.erp_cust_az12
FROM 'filepath'
WITH (FORMAT csv, HEADER true, DELIMITER ',', ENCODING 'UTF8');

-- ===========================================================
-- Load Product Category Data (ERP)
-- CSV contains: id, cat, subcat, maintenance
-- ===========================================================
\copy bronze.erp_px_cat_g1v2
FROM 'filepath'
WITH (FORMAT csv, HEADER true, DELIMITER ',', ENCODING 'UTF8');
