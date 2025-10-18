
-- ============================================================
-- Procedure: sp_load_bronze_layer
-- Purpose: Load all Bronze layer CSVs into MySQL tables
-- Author: Sagar Meravi
-- ============================================================

DELIMITER //

CREATE PROCEDURE sp_load_bronze_layer()
BEGIN
    DECLARE start_time DATETIME;
    DECLARE end_time DATETIME;
    DECLARE duration_seconds INT;

    -- Record start time
    SET start_time = NOW();

    -- ============================================================
    -- 1️⃣ CRM Sales Details
    -- ============================================================
    DROP TABLE IF EXISTS crm_sales_details;
    CREATE TABLE crm_sales_details (
        sls_ord_num   VARCHAR(50),
        sls_prd_key   VARCHAR(50),
        sls_cust_id   INT,
        sls_order_dt  DATE,
        sls_ship_dt   DATE,
        sls_due_dt    DATE,
        sls_sales     DECIMAL(10,2),
        sls_quantity  INT,
        sls_price     DECIMAL(10,2)
    );

    LOAD DATA LOCAL INFILE
    'D:/Data Analytics/Datawarehousing/sql-data-warehouse-project/datasets/source_crm/sales_details.csv'
    INTO TABLE crm_sales_details
    FIELDS TERMINATED BY ','
    ENCLOSED BY '"'
    LINES TERMINATED BY '\n'
    IGNORE 1 LINES
    (sls_ord_num, sls_prd_key, sls_cust_id, sls_order_dt, sls_ship_dt, sls_due_dt, sls_sales, sls_quantity, sls_price);


    -- ============================================================
    -- 2️⃣ ERP Location (A101)
    -- ============================================================
    DROP TABLE IF EXISTS erp_loc_a101;
    CREATE TABLE erp_loc_a101 (
        cid   VARCHAR(50),
        cntry VARCHAR(100)
    );

    LOAD DATA LOCAL INFILE
    'D:/Data Analytics/Datawarehousing/sql-data-warehouse-project/datasets/source_erp/loc_a101.csv'
    INTO TABLE erp_loc_a101
    FIELDS TERMINATED BY ','
    ENCLOSED BY '"'
    LINES TERMINATED BY '\n'
    IGNORE 1 LINES
    (cid, cntry);


    -- ============================================================
    -- 3️⃣ ERP Customer (AZ12)
    -- ============================================================
    DROP TABLE IF EXISTS erp_cust_az12;
    CREATE TABLE erp_cust_az12 (
        cid   VARCHAR(50),
        bdate DATE,
        gen   VARCHAR(20)
    );

    LOAD DATA LOCAL INFILE
    'D:/Data Analytics/Datawarehousing/sql-data-warehouse-project/datasets/source_erp/cust_az12.csv'
    INTO TABLE erp_cust_az12
    FIELDS TERMINATED BY ','
    ENCLOSED BY '"'
    LINES TERMINATED BY '\n'
    IGNORE 1 LINES
    (cid, bdate, gen);


    -- ============================================================
    -- 4️⃣ ERP Product Category (PX_CAT_G1V2)
    -- ============================================================
    DROP TABLE IF EXISTS erp_px_cat_g1v2;
    CREATE TABLE erp_px_cat_g1v2 (
        id           VARCHAR(50),
        cat          VARCHAR(100),
        subcat       VARCHAR(100),
        maintenance  VARCHAR(50)
    );

    LOAD DATA LOCAL INFILE
    'D:/Data Analytics/Datawarehousing/sql-data-warehouse-project/datasets/source_erp/px_cat_g1v2.csv'
    INTO TABLE erp_px_cat_g1v2
    FIELDS TERMINATED BY ','
    ENCLOSED BY '"'
    LINES TERMINATED BY '\n'
    IGNORE 1 LINES
    (id, cat, subcat, maintenance);


    -- ============================================================
    -- 5️⃣ CRM Customer Info
    -- ============================================================
    DROP TABLE IF EXISTS crm_cust_info;
    CREATE TABLE crm_cust_info (
        cst_id             INT,
        cst_key            VARCHAR(50),
        cst_firstname      VARCHAR(100),
        cst_lastname       VARCHAR(100),
        cst_marital_status VARCHAR(50),
        cst_gndr           VARCHAR(20),
        cst_create_date    DATE
    );

    LOAD DATA LOCAL INFILE
    'D:/Data Analytics/Datawarehousing/sql-data-warehouse-project/datasets/source_crm/cust_info.csv'
    INTO TABLE crm_cust_info
    FIELDS TERMINATED BY ','
    ENCLOSED BY '"'
    LINES TERMINATED BY '\n'
    IGNORE 1 LINES
    (cst_id, cst_key, cst_firstname, cst_lastname, cst_marital_status, cst_gndr, cst_create_date);


    -- Record end time
    SET end_time = NOW();

    -- Calculate duration in seconds
    SET duration_seconds = TIMESTAMPDIFF(SECOND, start_time, end_time);

    -- Display results
    SELECT
        start_time AS Load_Started_At,
        end_time AS Load_Ended_At,
        duration_seconds AS Duration_in_Seconds,
        CONCAT('Bronze Layer successfully loaded in ', duration_seconds, ' seconds') AS Status;

END //

DELIMITER ;

-- for run the procedure execute the below command
CALL sp_load_bronze_layer();
