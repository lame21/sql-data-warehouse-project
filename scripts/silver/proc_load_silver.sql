-- ============================================================
-- Procedure: silver.load_silver
-- Purpose  : Load and transform data from the bronze layer 
--            into the silver layer (cleaned & standardized).
-- Notes    :
--   * Each target table in silver is truncated before reload.
--   * Data transformations include trimming, mapping values,
--     type casting, and deriving surrogate fields.
--   * Execution times are logged with RAISE NOTICE.
-- ============================================================

CREATE OR REPLACE PROCEDURE silver.load_silver()
LANGUAGE plpgsql
AS $$
DECLARE
    start_time       timestamp;       -- Track start time of each table load
    end_time         timestamp;       -- Track end time of each table load
    batch_start_time timestamp;       -- Track overall batch start time
    batch_end_time   timestamp;       -- Track overall batch end time
BEGIN
    batch_start_time := clock_timestamp();

    RAISE NOTICE '>> LOADING SILVER LAYER';
    RAISE NOTICE '>> LOADING CRM & ERP TABLES';

    ------------------------------------------------------------------
    -- Load silver.crm_cust_info
    -- Deduplicates by cst_id (keeps latest record), standardizes marital
    -- status and gender, trims whitespace, and ensures clean output.
    ------------------------------------------------------------------
    start_time := clock_timestamp();
    RAISE NOTICE '>> Truncating Table: silver.crm_cust_info';
    TRUNCATE TABLE silver.crm_cust_info;

    RAISE NOTICE '>> Inserting Data Into: silver.crm_cust_info';
    INSERT INTO silver.crm_cust_info(
        cst_id, cst_key, cst_firstname, cst_lastname,
        cst_marital_status, cst_gndr, cst_create_date
    )
    SELECT
        cst_id,
        cst_key,
        TRIM(cst_firstname),
        TRIM(cst_lastname),
        -- Map marital status codes
        CASE 
            WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
            WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
            ELSE 'unknown'
        END,
        -- Map gender codes
        CASE
            WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
            WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
            ELSE 'Unknown'
        END,
        cst_create_date
    FROM (
        SELECT *,
               ROW_NUMBER() OVER(
                   PARTITION BY cst_id 
                   ORDER BY cst_create_date DESC
               ) AS flag_last
        FROM bronze.crm_cust_info
        WHERE cst_id IS NOT NULL
    ) t 
    WHERE flag_last = 1;   -- Keep only latest record per customer

    end_time := clock_timestamp();
    RAISE NOTICE '>> crm_cust_info loaded in % seconds (ended at %)', 
                 EXTRACT(EPOCH FROM (end_time - start_time)), end_time;

    ------------------------------------------------------------------
    -- Load silver.crm_prd_info
    -- Cleans product key, derives category, maps product line,
    -- calculates product end dates using LEAD window function.
    ------------------------------------------------------------------
    start_time := clock_timestamp();
    RAISE NOTICE '>> Truncating Table: silver.crm_prd_info';
    TRUNCATE TABLE silver.crm_prd_info;

    RAISE NOTICE '>> Inserting Data Into: silver.crm_prd_info';
    INSERT INTO silver.crm_prd_info(
        prd_id, cat_id, prd_key, prd_nm,
        prd_cost, prd_line, prd_start_dt, prd_end_dt
    )
    SELECT
        prd_id,
        REPLACE(SUBSTRING(prd_key, 1,5), '-', '_'),   -- Normalize category ID
        SUBSTRING(prd_key, 7, LENGTH(prd_key)),       -- Extract product key suffix
        prd_nm,
        COALESCE(prd_cost, 0),                        -- Replace NULL cost with 0
        CASE UPPER(TRIM(prd_line)) 
            WHEN 'M' THEN 'Mountain'
            WHEN 'R' THEN 'Road'
            WHEN 'S' THEN 'Other Sales'
            WHEN 'T' THEN 'Touring'
            ELSE 'Unknown'
        END,
        prd_start_dt,
        -- Calculate end date as the day before the next start date
        LEAD(prd_start_dt) OVER(
            PARTITION BY prd_key 
            ORDER BY prd_start_dt
        ) - interval '1 day'
    FROM bronze.crm_prd_info;

    end_time := clock_timestamp();
    RAISE NOTICE '>> crm_prd_info loaded in % seconds (ended at %)', 
                 EXTRACT(EPOCH FROM (end_time - start_time)), end_time;

    ------------------------------------------------------------------
    -- Load silver.crm_sales_details
    -- Cleans dates, recalculates sales if mismatched,
    -- ensures valid pricing and null handling.
    ------------------------------------------------------------------
    start_time := clock_timestamp();
    RAISE NOTICE '>> Truncating Table: silver.crm_sales_details';
    TRUNCATE TABLE silver.crm_sales_details;

    RAISE NOTICE '>> Inserting Data Into: silver.crm_sales_details';
    INSERT INTO silver.crm_sales_details(
        sls_ord_num, sls_prd_key, sls_cust_id,
        sls_order_dt, sls_ship_dt, sls_due_dt,
        sls_sales, sls_quantity, sls_price
    )
    SELECT 
        sls_ord_num,
        sls_prd_key,
        sls_cust_id,
        -- Validate and convert order date
        CASE
            WHEN sls_order_dt = 0 OR LENGTH(sls_order_dt::text) != 8 THEN NULL
            ELSE TO_DATE(sls_order_dt::text, 'YYYYMMDD')
        END,
        -- Validate and convert ship date
        CASE
            WHEN sls_ship_dt = 0 OR LENGTH(sls_ship_dt::text) != 8 THEN NULL
            ELSE TO_DATE(sls_ship_dt::text, 'YYYYMMDD')
        END,
        -- Validate and convert due date
        CASE
            WHEN sls_due_dt = 0 OR LENGTH(sls_due_dt::text) != 8 THEN NULL
            ELSE TO_DATE(sls_due_dt::text, 'YYYYMMDD')
        END,
        -- Recalculate sales if inconsistent
        CASE 
            WHEN sls_sales IS NULL OR sls_sales != sls_quantity * ABS(sls_price)
                THEN sls_quantity * ABS(sls_price)
            ELSE sls_sales
        END,
        sls_quantity,
        -- Correct invalid price
        CASE 
            WHEN sls_price IS NULL OR sls_price <= 0
                THEN sls_sales / NULLIF(sls_quantity,0)
            ELSE sls_price
        END
    FROM bronze.crm_sales_details;

    end_time := clock_timestamp();
    RAISE NOTICE '>> crm_sales_details loaded in % seconds (ended at %)', 
                 EXTRACT(EPOCH FROM (end_time - start_time)), end_time;

    ------------------------------------------------------------------
    -- Load silver.erp_cust_az12
    -- Cleans customer IDs, validates birthdates, standardizes gender.
    ------------------------------------------------------------------
    start_time := clock_timestamp();
    RAISE NOTICE '>> Truncating Table: silver.erp_cust_az12';
    TRUNCATE TABLE silver.erp_cust_az12;

    RAISE NOTICE '>> Inserting Data Into: silver.erp_cust_az12';
    INSERT INTO silver.erp_cust_az12(
        cid, bdate, gen
    )
    SELECT
        CASE 
            WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4) -- Strip NAS prefix
            ELSE cid
        END,
        CASE 
            WHEN bdate > CURRENT_DATE THEN NULL          -- Remove future birthdates
            ELSE bdate
        END,
        CASE 
            WHEN UPPER(TRIM(gen)) IN ('F','FEMALE') THEN 'Female'
            WHEN UPPER(TRIM(gen)) IN ('M','MALE')   THEN 'Male'
            ELSE 'N/A'
        END
    FROM bronze.erp_cust_az12;

    end_time := clock_timestamp();
    RAISE NOTICE '>> erp_cust_az12 loaded in % seconds (ended at %)', 
                 EXTRACT(EPOCH FROM (end_time - start_time)), end_time;

    ------------------------------------------------------------------
    -- Load silver.erp_loc_a101
    -- Cleans customer IDs, standardizes country codes/names.
    ------------------------------------------------------------------
    start_time := clock_timestamp();
    RAISE NOTICE '>> Truncating Table: silver.erp_loc_a101';
    TRUNCATE TABLE silver.erp_loc_a101;

    RAISE NOTICE '>> Inserting Data Into: silver.erp_loc_a101';
    INSERT INTO silver.erp_loc_a101(
        cid, cntry
    )
    SELECT 
        REPLACE(cid, '-',''),   -- Remove hyphens from customer ID
        CASE 
            WHEN TRIM(cntry) = 'DE' THEN 'Germany'
            WHEN TRIM(cntry) IN ('US','USA') THEN 'United States'
            WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'N/A'
            ELSE TRIM(cntry)
        END
    FROM bronze.erp_loc_a101;

    end_time := clock_timestamp();
    RAISE NOTICE '>> erp_loc_a101 loaded in % seconds (ended at %)', 
                 EXTRACT(EPOCH FROM (end_time - start_time)), end_time;

    ------------------------------------------------------------------
    -- Load silver.erp_px_cat_g1v2
    -- Simple copy from bronze to silver (no transformations).
    ------------------------------------------------------------------
    start_time := clock_timestamp();
    RAISE NOTICE '>> Truncating Table: silver.erp_px_cat_g1v2';
    TRUNCATE TABLE silver.erp_px_cat_g1v2;

    RAISE NOTICE '>> Inserting Data Into: silver.erp_px_cat_g1v2';
    INSERT INTO silver.erp_px_cat_g1v2(
        id, cat, subcat, maintenance
    )
    SELECT id, cat, subcat, maintenance
    FROM bronze.erp_px_cat_g1v2;

    end_time := clock_timestamp();
    RAISE NOTICE '>> erp_px_cat_g1v2 loaded in % seconds (ended at %)', 
                 EXTRACT(EPOCH FROM (end_time - start_time)), end_time;

    ------------------------------------------------------------------
    -- End of batch
    ------------------------------------------------------------------
    batch_end_time := clock_timestamp();
    RAISE NOTICE '>> Total Batch Duration: % seconds (ended at %)', 
                 EXTRACT(EPOCH FROM (batch_end_time - batch_start_time)), batch_end_time;

END;
$$;

-- Execute the procedure
CALL silver.load_silver();
