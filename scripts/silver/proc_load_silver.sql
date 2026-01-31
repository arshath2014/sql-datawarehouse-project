/*
================================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
================================================================================

Script Purpose:
    This stored procedure performs the ETL (Extract, Transform, Load) process to
    populate the 'silver' schema tables from the 'bronze' schema.

Actions Performed:
    - Truncates Silver tables.
    - Inserts transformed and cleansed data from Bronze into Silver tables.

Parameters:
    None.
    This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC silver.load_silver;

================================================================================
*/




CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN

DECLARE @start_time DATETIME,
        @end_time DATETIME;

DECLARE @layer_start_time DATETIME,
        @layer_end_time DATETIME;

BEGIN TRY

 SET @layer_start_time=GETDATE();
        PRINT '======================';
        PRINT 'LOADING SILVER LAYER';
        PRINT '======================';
   /* CRM CUSTOMER */
        SET @start_time = GETDATE();
/* ============================================================
   SILVER.CRM_CUST_INFO
============================================================ */

PRINT '>> Truncating table: silver.crm_cust_info';
TRUNCATE TABLE silver.crm_cust_info;

PRINT '>> Inserting data into: silver.crm_cust_info';

WITH Dedup AS (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY cst_id
               ORDER BY cst_create_date
           ) AS rn
    FROM bronze.crm_cust_info
    WHERE cst_id IS NOT NULL
)
INSERT INTO silver.crm_cust_info (
    cst_id,
    cst_key,
    cst_firstname,
    cst_lastname,
    cst_marital_status,
    cst_gndr,
    cst_create_date
)
SELECT
    cst_id,
    cst_key,
    TRIM(cst_firstname),
    TRIM(cst_lastname),
    CASE
        WHEN UPPER(TRIM(cst_material_status)) = 'S' THEN 'Single'
        WHEN UPPER(TRIM(cst_material_status)) = 'M' THEN 'Married'
        ELSE 'N/A'
    END,
    CASE
        WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
        WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
        ELSE 'N/A'
    END,
    cst_create_date
FROM Dedup
WHERE rn = 1;

        SET @end_time = GETDATE();
PRINT '>>Load duration of crm_cust_info:'+CAST(DATEDIFF(SECOND,@start_time,@end_time) AS VARCHAR)+'secs';
 PRINT '---------------------------';
/* ============================================================
   SILVER.CRM_PRD_INFO
============================================================ */


  SET @start_time = GETDATE();

        
PRINT '>> Truncating table: silver.crm_prd_info';
TRUNCATE TABLE silver.crm_prd_info;

PRINT '>> Inserting data into: silver.crm_prd_info';



INSERT INTO silver.crm_prd_info (
    prd_id,

    cat_id,
    pr_key,
    prd_nm,
    prd_cost,
    prd_line,
    prd_start_dt,
    prd_end_dt
)
SELECT
    prd_id,
    
    REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_'),
    SUBSTRING(prd_key, 7, LEN(prd_key)),
    TRIM(prd_nm),
    ISNULL(prd_cost, 0),
    CASE
        WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
        WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
        WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other Sales'
        WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
        ELSE 'N/A'
    END,
    CAST(prd_start_dt AS DATE),
    CAST(
        LEAD(prd_start_dt) OVER (
            PARTITION BY prd_key
            ORDER BY prd_start_dt
        ) - 1 AS DATE
    )
FROM bronze.crm_prd_info;


SET @end_time = GETDATE();
PRINT '>>Load duration of crm_prd_info:'+CAST(DATEDIFF(SECOND,@start_time,@end_time) AS VARCHAR)+'secs';
 PRINT '---------------------------';

/* ============================================================
   SILVER.CRM_SALES_DETAILS
============================================================ */

SET @start_time = GETDATE();

   


PRINT '>> Truncating table: silver.crm_sales_details';
TRUNCATE TABLE silver.crm_sales_details;

PRINT '>> Inserting data into: silver.crm_sales_details';

INSERT INTO silver.crm_sales_details (
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
        WHEN LEN(sls_order_dt) = 8 AND sls_order_dt > 0
        THEN CAST(CAST(sls_order_dt AS VARCHAR(8)) AS DATE)
        ELSE NULL
    END,
    CASE
        WHEN LEN(sls_ship_dt) = 8 AND sls_ship_dt > 0
        THEN CAST(CAST(sls_ship_dt AS VARCHAR(8)) AS DATE)
        ELSE NULL
    END,
    CASE
        WHEN LEN(sls_due_dt) = 8 AND sls_due_dt > 0
        THEN CAST(CAST(sls_due_dt AS VARCHAR(8)) AS DATE)
        ELSE NULL
    END,
    CASE
        WHEN sls_sales IS NULL
          OR sls_sales <= 0
          OR sls_sales <> sls_quantity * ABS(sls_price)
        THEN sls_quantity * ABS(sls_price)
        ELSE sls_sales
    END,
    sls_quantity,
    CASE
        WHEN sls_price IS NULL OR sls_price <= 0
        THEN sls_sales / NULLIF(sls_quantity, 0)
        ELSE sls_price
    END
FROM bronze.crm_sales_details;


SET @end_time = GETDATE();

PRINT '>>Load duration of crm_sales_details:'+CAST(DATEDIFF(SECOND,@start_time,@end_time) AS VARCHAR)+'secs';
 PRINT '---------------------------';

/* ============================================================
   SILVER.ERP_CUST_AZ12
============================================================ */

SET @start_time=GETDATE();

PRINT '>> Truncating table: silver.erp_cust_az12';
TRUNCATE TABLE silver.erp_cust_az12;

PRINT '>> Inserting data into: silver.erp_cust_az12';

INSERT INTO silver.erp_cust_az12 (
    cid,
    bdate,
    gen
)
SELECT
    CASE
        WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
        ELSE cid
    END,
    CASE
        WHEN bdate < '1924-01-01'
          OR bdate > GETDATE()
        THEN NULL
        ELSE bdate
    END,
    CASE
        WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
        WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
        ELSE 'N/A'
    END
FROM bronze.erp_cust_az12;

SET @end_time = GETDATE();

PRINT '>>Load duration of erp_cust_az12:'+CAST(DATEDIFF(SECOND,@start_time,@end_time) AS VARCHAR)+'secs';
 PRINT '---------------------------';

/* ============================================================
   SILVER.ERP_LOC_A101
============================================================ */

SET @start_time =GETDATE();

PRINT '>> Truncating table: silver.erp_loc_a101';
TRUNCATE TABLE silver.erp_loc_a101;

PRINT '>> Inserting data into: silver.erp_loc_a101';

INSERT INTO silver.erp_loc_a101 (
    cid,
    cntry
)
SELECT
    REPLACE(cid, '-', ''),
    CASE
        WHEN TRIM(cntry) = 'DE' THEN 'Germany'
        WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
        WHEN cntry IS NULL OR TRIM(cntry) = '' THEN 'N/A'
        ELSE TRIM(cntry)
    END
FROM bronze.erp_loc_a101;

SET @end_time = GETDATE();

PRINT '>>Load duration of erp_loc_a101:'+CAST(DATEDIFF(SECOND,@start_time,@end_time) AS VARCHAR)+'secs';
 PRINT '---------------------------';

/* ============================================================
   SILVER.ERP_PX_CAT_G1V2
============================================================ */
SET @start_time=GETDATE();

PRINT '>> Truncating table: silver.erp_px_cat_g1v2';
TRUNCATE TABLE silver.erp_px_cat_g1v2;

PRINT '>> Inserting data into: silver.erp_px_cat_g1v2';

INSERT INTO silver.erp_px_cat_g1v2 (
    id,
    cat,
    subcat,
    maintenance
)
SELECT
    id,
    TRIM(cat),
    TRIM(subcat),
    TRIM(maintenance)
FROM bronze.erp_px_cat_g1v2;

SET @end_time = GETDATE();

PRINT '>>Load duration of erp_px_cat_g1v2:'+CAST(DATEDIFF(SECOND,@start_time,@end_time) AS VARCHAR)+'secs';
 PRINT '---------------------------';

 SET @layer_end_time=GETDATE();

 PRINT '=======================================';

PRINT '>>LOAD DURATION OF SILVER LAYER:'+CAST(DATEDIFF(SECOND,@layer_start_time,@layer_end_time) AS VARCHAR)+'secs';



END TRY
    BEGIN CATCH
        PRINT '=======================================';
        PRINT 'ERROR OCCURRED WHILE LOADING SILVER LAYER';
        PRINT 'Error Message: ' + ERROR_MESSAGE();
        PRINT 'Error Number: ' + CAST(ERROR_NUMBER() AS NVARCHAR(10));
        PRINT 'Error State: ' + CAST(ERROR_STATE() AS NVARCHAR(10));
        PRINT 'Error Line: ' + CAST(ERROR_LINE() AS NVARCHAR(10));
        PRINT 'Error Procedure: ' + ISNULL(ERROR_PROCEDURE(), 'N/A');
    END CATCH



END;
GO

EXEC silver.load_silver;



