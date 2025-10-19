-- =========================================================================
-- SQL SCRIPT: BRONZE TO SILVER DATA TRANSFORMATION
-- =========================================================================
-- This script truncates and repopulates the Silver layer tables
-- from the Bronze layer, applying business rules and data cleaning.
-- =========================================================================


-- ---------------------------------
-- Table: crm_cust_info
-- ---------------------------------
-- Purpose: Cleans and transforms customer data.
-- 1. Deduplicates records, keeping only the latest one per cst_id.
-- 2. Trims whitespace from names.
-- 3. Maps marital status and gender codes to full text.
-- ---------------------------------

TRUNCATE TABLE silver_datawarehouse.crm_cust_info;

INSERT INTO silver_datawarehouse.crm_cust_info (cst_id,
                                                cst_key,
                                                cst_firstname,
                                                cst_lastname,
                                                cst_marital_status,
                                                cst_gndr,
                                                cst_create_date)
SELECT cst_id,
       cst_key,
       TRIM(cst_firstname) AS cst_firstname,
       TRIM(cst_lastname)  AS cst_lastname,
       CASE
           WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
           WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
           ELSE 'n/a'
           END             AS cst_marital_status,
       CASE
           WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
           WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
           ELSE 'n/a'
           END             AS cst_gndr,
       cst_create_date
FROM (SELECT *,
             ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
      FROM bronze_datawarehouse.crm_cust_info) t
WHERE flag_last = 1;


-- ---------------------------------
-- Table: crm_prd_info
-- ---------------------------------
-- Purpose: Cleans and transforms product information.
-- 1. Extracts cat_id and prd_key from the concatenated prd_key.
-- 2. Handles NULL costs.
-- 3. Maps product line codes to full text.
-- 4. Calculates prd_end_dt for Type 2 Slowly Changing Dimension (SCD).
-- ---------------------------------

TRUNCATE TABLE silver_datawarehouse.crm_prd_info;

INSERT INTO silver_datawarehouse.crm_prd_info (
    prd_id,
    cat_id,
    prd_key,
    prd_nm,
    prd_cost,
    prd_line,
    prd_start_dt,
    prd_end_dt
)
SELECT
    prd_id,
    REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,     -- Extract category ID
    SUBSTRING(prd_key, 7, LENGTH(prd_key)) AS prd_key,          -- Extract product key
    prd_nm,
    IFNULL(prd_cost, 0) AS prd_cost,
    CASE
        WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
        WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
        WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other Sales'
        WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
        ELSE 'n/a'
    END AS prd_line,                                            -- Map product line codes
    CAST(prd_start_dt AS DATE) AS prd_start_dt,
    CAST(
        DATE_SUB(
            LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt),
            INTERVAL 1 DAY
        ) AS DATE
    ) AS prd_end_dt                                             -- End date = 1 day before next start date
FROM bronze_datawarehouse.crm_prd_info;


-- ---------------------------------
-- Table: crm_sales_details
-- ---------------------------------
-- Purpose: Cleans and transforms sales transaction data.
-- 1. Converts integer-based dates (Ymd) to proper DATE format.
-- 2. Recalculates sls_sales if it's invalid or NULL.
-- 3. Recalculates sls_price if it's invalid or NULL.
-- ---------------------------------

TRUNCATE TABLE silver_datawarehouse.crm_sales_details;

INSERT INTO silver_datawarehouse.crm_sales_details (
    sls_ord_num,
    sls_prd_key,
    sls_cust_id,
    sls_order_dt,
    sls_ship_dt,
    sls_due_dt,
    sls_sales,
    sls_quantity,
    sls_price
)
SELECT
    sls_ord_num,
    sls_prd_key,
    sls_cust_id,

    CASE
        WHEN sls_order_dt = 0 OR LENGTH(sls_order_dt) != 8 THEN NULL
        ELSE STR_TO_DATE(sls_order_dt, '%Y%m%d')
    END AS sls_order_dt,

    CASE
        WHEN sls_ship_dt = 0 OR LENGTH(sls_ship_dt) != 8 THEN NULL
        ELSE STR_TO_DATE(sls_ship_dt, '%Y%m%d')
    END AS sls_ship_dt,

    CASE
        WHEN sls_due_dt = 0 OR LENGTH(sls_due_dt) != 8 THEN NULL
        ELSE STR_TO_DATE(sls_due_dt, '%Y%m%d')
    END AS sls_due_dt,

    CASE
        WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price)
            THEN sls_quantity * ABS(sls_price)
        ELSE sls_sales
    END AS sls_sales,
    sls_quantity,

    CASE
        WHEN sls_price IS NULL OR sls_price <= 0
            THEN CASE WHEN sls_quantity != 0 THEN sls_sales / sls_quantity ELSE NULL END
        ELSE sls_price
    END AS sls_price

FROM bronze_datawarehouse.crm_sales_details;


-- ---------------------------------
-- Table: erp_cust_az12
-- ---------------------------------
-- Purpose: Cleans customer data from an ERP system.
-- 1. Removes 'NAS' prefix from customer IDs.
-- 2. Nullifies invalid future birthdates.
-- 3. Cleans and maps gender codes (handling 'F', 'M', and extra characters).
-- ---------------------------------

TRUNCATE TABLE silver_datawarehouse.erp_cust_az12;

INSERT INTO silver_datawarehouse.erp_cust_az12 (
    cid,
    bdate,
    gen
)
SELECT
    CASE
        WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LENGTH(cid))
        ELSE cid
    END AS cid,

    CASE
        WHEN bdate > CURRENT_DATE() THEN NULL
        ELSE bdate
    END AS bdate,

    CASE
        WHEN UPPER(REPLACE(TRIM(gen), '\r', '')) LIKE 'F%' THEN 'Female'
        WHEN UPPER(REPLACE(TRIM(gen), '\r', '')) LIKE 'M%' THEN 'Male'
        ELSE 'n/a'
    END AS gen
FROM bronze_datawarehouse.erp_cust_az12;


-- ---------------------------------
-- Table: erp_loc_a101
-- ---------------------------------
-- Purpose: Cleans location data from an ERP system.
-- 1. Removes hyphens from customer IDs.
-- 2. Maps country codes ('DE', 'US', 'USA') to full names.
-- 3. Handles blank or NULL country codes.
-- ---------------------------------

TRUNCATE TABLE silver_datawarehouse.erp_loc_a101;

INSERT INTO silver_datawarehouse.erp_loc_a101 (
    cid,
    cntry
)
SELECT
    -- Remove hyphens from customer ID
    REPLACE(cid, '-', '') AS cid,
    CASE
        WHEN TRIM(cntry) = 'DE' THEN 'Germany'
        WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
        WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
        ELSE TRIM(cntry)
    END AS cntry
FROM bronze_datawarehouse.erp_loc_a101;


-- ---------------------------------
-- Table: erp_px_cat_g1v2
-- ---------------------------------
-- Purpose: Loads product category data.
-- 1. This is a direct 1:1 load with no transformations.
-- ---------------------------------

TRUNCATE TABLE silver_datawarehouse.erp_px_cat_g1v2;

INSERT INTO silver_datawarehouse.erp_px_cat_g1v2 (
          id,
          cat,
          subcat,
          maintenance
       )
       SELECT
          id,
          cat,
          subcat,
          maintenance
       FROM bronze_datawarehouse.erp_px_cat_g1v2;


-- ---------------------------------
-- Verification
-- ---------------------------------
-- The last query from the original prompt is included below.
-- You can uncomment it to verify the data load for the final table.
-- ---------------------------------
-- SELECT * FROM silver_datawarehouse.erp_px_cat_g1v2;

-- =========================================================================
-- END OF SCRIPT
-- =========================================================================
