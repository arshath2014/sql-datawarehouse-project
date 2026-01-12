/*
================================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
================================================================================

Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files.
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the `BULK INSERT` command to load data from csv Files to bronze tables.

Parameters:
    None.
    This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC bronze.load_bronze;

================================================================================
*/


CREATE OR ALTER PROCEDURE bronze.load_bronze
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @start_time DATETIME, 
            @end_time   DATETIME;
     DECLARE @layer_start_time DATETIME, 
            @layer_end_time   DATETIME;

    BEGIN TRY
      
       SET @layer_start_time=GETDATE();
        PRINT '======================';
        PRINT 'LOADING BRONZE LAYER';
        PRINT '======================';

        -----------------------------
        -- CRM TABLES
        -----------------------------
        PRINT 'Loading CRM tables';
        PRINT '-----------------------------';

        /* CRM CUSTOMER */
        SET @start_time = GETDATE();

        PRINT '>> Truncating Table: bronze.crm_cust_info';
        TRUNCATE TABLE bronze.crm_cust_info;

        PRINT '>> Loading Table: bronze.crm_cust_info';
        BULK INSERT bronze.crm_cust_info
        FROM 'D:\DATA WAREHOUSE\sql-data-warehouse-project\datasets\source_crm\cust_info.CSV'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );

        SET @end_time = GETDATE();
        PRINT '>>> Load duration: ' 
            + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS VARCHAR(10)) 
            + ' seconds';
        PRINT '--------------';

        /* CRM PRODUCT */
        SET @start_time = GETDATE();

        PRINT '>> Truncating Table: bronze.crm_prd_info';
        TRUNCATE TABLE bronze.crm_prd_info;

        PRINT '>> Loading Table: bronze.crm_prd_info';
        BULK INSERT bronze.crm_prd_info
        FROM 'D:\DATA WAREHOUSE\sql-data-warehouse-project\datasets\source_crm\prd_info.CSV'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );

        SET @end_time = GETDATE();
        PRINT '>>> Load duration: ' 
            + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS VARCHAR(10)) 
            + ' seconds';
        PRINT '--------------';

        /* CRM SALES */
        SET @start_time = GETDATE();

        PRINT '>> Truncating Table: bronze.crm_sales_details';
        TRUNCATE TABLE bronze.crm_sales_details;

        PRINT '>> Loading Table: bronze.crm_sales_details';
        BULK INSERT bronze.crm_sales_details
        FROM 'D:\DATA WAREHOUSE\sql-data-warehouse-project\datasets\source_crm\sales_details.CSV'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );

        SET @end_time = GETDATE();
        PRINT '>>> Load duration: ' 
            + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS VARCHAR(10)) 
            + ' seconds';
        PRINT '--------------';

        -----------------------------
        -- ERP TABLES
        -----------------------------
        PRINT 'Loading ERP tables';
        PRINT '-----------------------------';

        /* ERP CUSTOMER */
        SET @start_time = GETDATE();

        PRINT '>> Truncating Table: bronze.erp_cust_az12';
        TRUNCATE TABLE bronze.erp_cust_az12;

        PRINT '>> Loading Table: bronze.erp_cust_az12';
        BULK INSERT bronze.erp_cust_az12
        FROM 'D:\DATA WAREHOUSE\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.CSV'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );

        SET @end_time = GETDATE();
        PRINT '>>> Load duration: ' 
            + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS VARCHAR(10)) 
            + ' seconds';
        PRINT '--------------';

        /* ERP LOCATION */
        SET @start_time = GETDATE();

        PRINT '>> Truncating Table: bronze.erp_loc_a101';
        TRUNCATE TABLE bronze.erp_loc_a101;

        PRINT '>> Loading Table: bronze.erp_loc_a101';
        BULK INSERT bronze.erp_loc_a101
        FROM 'D:\DATA WAREHOUSE\sql-data-warehouse-project\datasets\source_erp\loc_a101.CSV'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );

        SET @end_time = GETDATE();
        PRINT '>>> Load duration: ' 
            + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS VARCHAR(10)) 
            + ' seconds';
        PRINT '--------------';

        /* ERP PRODUCT CATEGORY */
        SET @start_time = GETDATE();

        PRINT '>> Truncating Table: bronze.erp_px_cat_g1v2';
        TRUNCATE TABLE bronze.erp_px_cat_g1v2;

        PRINT '>> Loading Table: bronze.erp_px_cat_g1v2';
        BULK INSERT bronze.erp_px_cat_g1v2
        FROM 'D:\DATA WAREHOUSE\sql-data-warehouse-project\datasets\source_erp\px_cat_g1V2.CSV'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );

        SET @end_time = GETDATE();
        PRINT '>>> Load duration: ' 
            + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS VARCHAR(10)) 
            + ' seconds';
        PRINT '--------------';

        ------------------------------------
        PRINT 'BRONZE LAYER LOAD COMPLETED SUCCESSFULLY!';
        ------------------------------------
         SET @layer_end_time=GETDATE();
     PRINT '>>> Load duration for BRONZE LAYER: ' 
            + CAST(DATEDIFF(SECOND, @layer_start_time, @layer_end_time) AS VARCHAR)
            + ' seconds';
        PRINT '--------------';
    END TRY
    BEGIN CATCH
        PRINT '=======================================';
        PRINT 'ERROR OCCURRED WHILE LOADING BRONZE LAYER';
        PRINT 'Error Message: ' + ERROR_MESSAGE();
        PRINT 'Error Number: ' + CAST(ERROR_NUMBER() AS NVARCHAR(10));
        PRINT 'Error State: ' + CAST(ERROR_STATE() AS NVARCHAR(10));
        PRINT 'Error Line: ' + CAST(ERROR_LINE() AS NVARCHAR(10));
        PRINT 'Error Procedure: ' + ISNULL(ERROR_PROCEDURE(), 'N/A');
    END CATCH
   
END;
GO

EXEC bronze.load_bronze;
