/* ============================================================
   Data Catalog: Gold Layer
   ------------------------------------------------------------
   The Gold Layer represents the **business-level data models** 
   designed for reporting and analytics. 

   Key Features:
   - Dimension tables provide enriched attributes 
     (e.g., customers, products).
   - Fact tables store transactional metrics 
     (e.g., sales).

   Purpose of Views:
   - gold.dim_customers: Customer dimension with demographics 
     and geographic attributes.
   - gold.dim_products: Product dimension enriched with 
     category and cost details.
   - gold.fact_sales: Sales fact table linking customers and 
     products to transactional measures.

   Together, these views support BI dashboards, ad-hoc 
   analysis, and business reporting.
   ============================================================ */


/* ============================================================
   View: gold.dim_customers
   Purpose: Dimension table for customer details.
   Source: silver.crm_cust_info, silver.erp_cust_az12, silver.erp_loc_a101
   Notes:
   - Generates surrogate key with ROW_NUMBER().
   - Enriches customer data with demographics and location.
   ============================================================ */
CREATE VIEW gold.dim_customers AS (
    SELECT
        ROW_NUMBER() OVER (ORDER BY cst_id) AS customer_key,  -- Surrogate key
        ci.cst_id AS customer_id,                            -- Unique customer ID
        ci.cst_key AS customer_number,                       -- Customer number from CRM
        ci.cst_firstname AS first_name,                      -- First name
        ci.cst_lastname AS last_name,                        -- Last name
        la.cntry AS country,                                 -- Country from ERP location
        ci.cst_marital_status AS marital_status,             -- Marital status
        CASE 
            WHEN ci.cst_gndr != 'Unknown' THEN ci.cst_gndr   -- Prefer CRM gender
            ELSE COALESCE(ca.gen, 'N/A')                     -- Fallback to ERP gender
        END AS gender,
        ca.bdate AS birthdate,                               -- Birthdate from ERP
        ci.cst_create_date AS create_date                    -- Record creation date
    FROM silver.crm_cust_info ci
    LEFT JOIN silver.erp_cust_az12 ca
        ON ci.cst_key = ca.cid
    LEFT JOIN silver.erp_loc_a101 la
        ON ci.cst_key = la.cid
);




/* ============================================================
   View: gold.dim_products
   Purpose: Dimension table for product details.
   Source: silver.crm_prd_info, silver.erp_px_cat_g1v2
   Notes:
   - Surrogate key created with ROW_NUMBER().
   - Joins product info with category details.
   - Filters out products with an end date (historical data).
   ============================================================ */
CREATE VIEW gold.dim_products AS
    SELECT
        ROW_NUMBER() OVER(ORDER BY pn.prd_start_dt, pn.prd_key) AS product_key, -- Surrogate key
        pn.prd_id AS product_id,                                               -- Unique product ID
        pn.prd_key AS product_number,                                          -- Product number
        pn.prd_nm AS product_name,                                             -- Product name
        pn.cat_id AS category_id,                                              -- Category ID
        pc.cat AS category,                                                    -- Category name
        pc.subcat AS subcategory,                                              -- Subcategory name
        pc.maintenance,                                                        -- Maintenance flag
        pn.prd_cost AS cost,                                                   -- Product cost
        pn.prd_line AS product_line,                                           -- Product line
        pn.prd_start_dt AS start_date                                          -- Availability start date
    FROM silver.crm_prd_info pn
    LEFT JOIN silver.erp_px_cat_g1v2 pc 
        ON pn.cat_id = pc.id
    WHERE pn.prd_end_dt IS NULL;  -- Exclude historical products




/* ============================================================
   View: gold.fact_sales
   Purpose: Fact table for sales transactions.
   Source: silver.crm_sales_details, gold.dim_products, gold.dim_customers
   Notes:
   - Links sales data to product and customer dimensions.
   - Stores transactional details (amount, quantity, price).
   ============================================================ */
CREATE VIEW gold.fact_sales AS
    SELECT
        sd.sls_ord_num AS order_number,       -- Sales order number
        pr.product_key,                       -- Surrogate product key
        cu.customer_key,                      -- Surrogate customer key
        sd.sls_order_dt AS order_date,        -- Order date
        sd.sls_ship_dt AS shipping_date,      -- Shipping date
        sd.sls_due_dt AS due_date,            -- Payment due date
        sd.sls_sales AS sales_amount,         -- Total sales amount
        sd.sls_quantity AS quantity,          -- Quantity ordered
        sd.sls_price AS price                 -- Price per unit
    FROM silver.crm_sales_details sd
    LEFT JOIN gold.dim_products pr
        ON sd.sls_prd_key = pr.product_number
    LEFT JOIN gold.dim_customers cu
        ON sd.sls_cust_id = cu.customer_id;
