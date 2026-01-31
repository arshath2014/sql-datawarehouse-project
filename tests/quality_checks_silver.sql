/*
================================================================================
Quality Checks
================================================================================

Script Purpose:
    This script performs various quality checks for data consistency, accuracy,
    and standardization across the 'silver' schema. It includes checks for:
    - Null or duplicate primary keys.
    - Unwanted spaces in string fields.
    - Data standardization and consistency.
    - Invalid date ranges and orders.
    - Data consistency between related fields.

Usage Notes:
    - Run these checks after data loading Silver Layer.
    - Investigate and resolve any discrepancies found during the checks.

================================================================================
*/


-- CHECKING DUPLICATES
SELECT 
    cst_id,
    COUNT(*) 
FROM bronze.crm_cust_INfo 
GROUP BY cst_id  

HAVING COUNT(*) > 1 
    OR cst_id IS NULL;



SELECT * 
FROM bronze.crm_cust_INfo;



SELECT * 
FROM (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY cst_id 
               ORDER BY cst_create_date
           ) AS flag
    FROM bronze.crm_cust_INfo
) AS t 
 WHERE flag = 1;

DELETE 
FROM bronze.crm_cust_INfo 
 WHERE cst_id IS NULL;

-- REMOVING WHITESPACES
SELECT cst_firstname 
FROM bronze.crm_cust_INfo 
 WHERE cst_firstname != TRIM(cst_firstname);

SELECT cst_lastname 
FROM bronze.crm_cust_INfo 
 WHERE cst_lastname != TRIM(cst_lastname);

SELECT cst_gndr 
FROM bronze.crm_cust_INfo 
 WHERE cst_gndr != TRIM(cst_gndr);

USE DATAWAREHOUSE;

PRINT '>> INsertINg data INto silver.crm_cust_INfo';

INSERT  INTO silver.crm_cust_INfo
(
    cst_id,
    cst_key,
    cst_firstname,
    cst_lastname,
    cst_material_status,
    cst_gndr,
    cst_create_date
)
SELECT
    cst_id,
    cst_key,
    TRIM(cst_firstname) AS cst_firstname,
    TRIM(cst_lastname)  AS cst_lastname,
    CASE 
        WHEN  UPPER(TRIM(cst_material_status)) = 'S' THEN  'SINgle'
        WHEN  UPPER(TRIM(cst_material_status)) = 'M' THEN  'Married'
        ELSE  'n/a'
    END AS  cst_material_status,
    CASE 
        WHEN  UPPER(TRIM(cst_gndr)) = 'F' THEN  'Female'
        WHEN  UPPER(TRIM(cst_gndr)) = 'M' THEN  'Male'
        ELSE  'n/a'
    END AS  cst_gndr,
    cst_create_date
FROM (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY cst_id 
               ORDER BY cst_create_date
           ) AS flag
    FROM bronze.crm_cust_INfo
) t
 WHERE flag = 1
  AND cst_id IS NOT NULL;

SELECT * 
FROM silver.crm_cust_INfo;


--crm_prd_INfo
--Data Standardiztion & Consistency
SELECT
    prd_id,
    prd_key,
    REPLACE(SUBSTRING(prd_key,1,5),'-','_') AS cat_id,
    SUBSTRING(prd_key,7,LEN(prd_key)) AS prd_key,
    TRIM(prd_nm) AS prd_nm,
    ISNULL(prd_cost,0) AS prd_cost,
    prd_lINe,
    prd_start_dt,
    prd_end_dt
FROM bronze.crm_prd_INfo
 WHERE  SUBSTRING(prd_key,7,LEN(prd_key)) IN (

SELECT sls_prd_key FROM bronze.crm_sales_details);


SELECT
    prd_id,
    prd_key,
    REPLACE(SUBSTRING(prd_key,1,5),'-','_') AS cat_id,
    SUBSTRING(prd_key,7,LEN(prd_key)) AS prd_key,
    TRIM(prd_nm) AS prd_nm,
    ISNULL(prd_cost,0) AS prd_cost,
    
     CASE 
        WHEN  UPPER(TRIM(prd_lINe)) = 'R' THEN  'Road'
        WHEN  UPPER(TRIM(prd_lINe)) = 'M' THEN  'MountaIN'
        WHEN  UPPER(TRIM(prd_lINe)) = 'S' THEN  'Other Sales'
        WHEN  UPPER(TRIM(prd_lINe)) = 'T' THEN  'TourINg'
        ELSE  'n/a'
    END AS  prd_lINe,
    CAST(prd_start_dt AS DATE) AS prd_start_dt,
    CAST(LEAD(prd_start_dt ) OVER(PARTITION BY prd_KEY ORDER BY prd_start_dt)-1 AS DATE) AS prd_end_dt
    
FROM bronze.crm_prd_INfo;

PRINT '>> INsertINg data INto silver.crm_prd_INfo';

INSERT  INTO silver.crm_prd_INfo
(
    prd_id,
    prd_key,
    cat_id,
    pr_key,
    prd_nm,
    prd_cost,
    prd_lINe,
    prd_start_dt,
    prd_end_dt
)
SELECT
    prd_id,
    prd_key,
    REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
    SUBSTRING(prd_key, 7, LEN(prd_key))          AS pr_key,
    TRIM(prd_nm)                                AS prd_nm,
    ISNULL(prd_cost, 0)                         AS prd_cost,
    CASE 
        WHEN  UPPER(TRIM(prd_lINe)) = 'R' THEN  'Road'
        WHEN  UPPER(TRIM(prd_lINe)) = 'M' THEN  'MountaIN'
        WHEN  UPPER(TRIM(prd_lINe)) = 'S' THEN  'Other Sales'
        WHEN  UPPER(TRIM(prd_lINe)) = 'T' THEN  'TourINg'
        ELSE  'n/a'
    END AS  prd_lINe,
    CAST(prd_start_dt AS DATE) AS prd_start_dt,
    CAST(
        LEAD(prd_start_dt) OVER (
            PARTITION BY prd_key 
            ORDER BY prd_start_dt
        ) - 1 AS DATE
    ) AS prd_end_dt
FROM bronze.crm_prd_INfo;


SELECT * FROM silver.crm_prd_INfo;

--verification sales data with customer id

SELECT   * FROM bronze.crm_sales_details  WHERE sls_cust_id  IN (SELECT cst_id FROM bronze.crm_cust_INfo);

SELECT   * FROM bronze.crm_sales_details  WHERE sls_cust_id not IN (SELECT cst_id FROM bronze.crm_cust_INfo);

SELECT
    sls_ord_num,
    sls_prd_key,
    sls_cust_id,
    sls_order_dt,
    sls_ship_dt,
    sls_due_dt,
    sls_sales,
    sls_quantity,
    sls_price
FROM bronze.crm_sales_details;

--check INvalid dates

SELECT NULLIF(sls_order_dt,0) 
FROM bronze.crm_sales_details
 WHERE sls_order_dt <0 
or LEN(sls_order_dt) !=8
or sls_order_dt > 20500101
or sls_order_dt < 19000101;


SELECT NULLIF(sls_ship_dt,0) 
FROM bronze.crm_sales_details
 WHERE sls_ship_dt <0 
or LEN(sls_ship_dt) !=8
or sls_ship_dt > 20500101
or sls_ship_dt < 19000101;


SELECT NULLIF( sls_due_dt,0) 
FROM bronze.crm_sales_details
 WHERE  sls_due_dt <0 
or LEN( sls_due_dt) !=8
or  sls_due_dt > 20500101
or  sls_due_dt < 19000101;

SELECT
    sls_ord_num,
    sls_prd_key,
    sls_cust_id,

    -- Clean Order Date
    CASE 
        WHEN  LEN(sls_order_dt) != 8 OR sls_order_dt < 0 THEN  NULL
        ELSE  CAST(CAST(sls_order_dt AS VARCHAR(8)) AS DATE)
    END AS  sls_order_dt,

    -- Clean Ship Date
    CASE 
        WHEN  LEN(sls_ship_dt) != 8 OR sls_ship_dt < 0 THEN  NULL
        ELSE  CAST(CAST(sls_ship_dt AS VARCHAR(8)) AS DATE)
    END AS  sls_ship_dt,

    -- Clean Due Date
    CASE 
        WHEN  LEN(sls_due_dt) != 8 OR sls_due_dt < 0 THEN  NULL
        ELSE  CAST(CAST(sls_due_dt AS VARCHAR(8)) AS DATE)
    END AS  sls_due_dt,

    sls_sales,
    sls_quantity,
    sls_price
FROM bronze.crm_sales_details;

--INvalid order dates (order must be earlier than ship and due date !)

SELECT * FROM bronze.crm_sales_details
 WHERE sls_order_dt > sls_ship_dt or sls_order_dt > sls_due_dt;


--SolvINg issues IN Sales, quantity and prices 
SELECT sls_sales,sls_quantity,sls_price FROM bronze.crm_sales_details 
 WHERE sls_sales <=0 
or sls_sales is null 
or sls_sales != sls_quantity * sls_price
or sls_quantity <=0 or  sls_quantity is null 
or sls_price <=0 or sls_price is null  
order by sls_sales;

use datawarehouse;

SELECT
    sls_ord_num,
    sls_prd_key,
    sls_cust_id,

    -- Clean Order Date
    CASE 
        WHEN  LEN(sls_order_dt) != 8 OR sls_order_dt < 0 THEN  NULL
        ELSE  CAST(CAST(sls_order_dt AS VARCHAR(8)) AS DATE)
    END AS  sls_order_dt,

    -- Clean Ship Date
    CASE 
        WHEN  LEN(sls_ship_dt) != 8 OR sls_ship_dt < 0 THEN  NULL
        ELSE  CAST(CAST(sls_ship_dt AS VARCHAR(8)) AS DATE)
    END AS  sls_ship_dt,

    -- Clean Due Date
    CASE 
        WHEN  LEN(sls_due_dt) != 8 OR sls_due_dt < 0 THEN  NULL
        ELSE  CAST(CAST(sls_due_dt AS VARCHAR(8)) AS DATE)
    END AS  sls_due_dt,1

    -----Corrected sales data
    CASE 
       WHEN  sls_sales IS NULL OR sls_sales<=0 OR sls_sales!=sls_quantity*ABS(sls_price) 
       THEN  sls_quantity*ABS(sls_price)
       ELSE  sls_sales
    END AS  sls_sales,

    sls_quantity,

    ------Corrected price data
    CASE 
       WHEN  sls_price IS NULL OR sls_price<=0 
       THEN  sls_sales/NULLIF(sls_quantity,0)
       ELSE  sls_price
    END AS  sls_price
    
FROM bronze.crm_sales_details;

-- INsertINg data INto silver.crm_sales_details 
INSERT  INTO silver.crm_sales_details
(
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

    -- Clean Order Date
    CASE 
        WHEN  LEN(sls_order_dt) != 8 OR sls_order_dt < 0 THEN  NULL
        ELSE  CAST(CAST(sls_order_dt AS VARCHAR(8)) AS DATE)
    END AS  sls_order_dt,

    -- Clean Ship Date
    CASE 
        WHEN  LEN(sls_ship_dt) != 8 OR sls_ship_dt < 0 THEN  NULL
        ELSE  CAST(CAST(sls_ship_dt AS VARCHAR(8)) AS DATE)
    END AS  sls_ship_dt,

    -- Clean Due Date
    CASE 
        WHEN  LEN(sls_due_dt) != 8 OR sls_due_dt < 0 THEN  NULL
        ELSE  CAST(CAST(sls_due_dt AS VARCHAR(8)) AS DATE)
    END AS  sls_due_dt,

    -----Corrected sales data
    CASE 
       WHEN  sls_sales IS NULL OR sls_sales<=0 OR sls_sales!=sls_quantity*ABS(sls_price) 
       THEN  sls_quantity*ABS(sls_price)
       ELSE  sls_sales
    END AS  sls_sales,

    sls_quantity,

    ------Corrected price data
    CASE 
       WHEN  sls_price IS NULL OR sls_price<=0 
       THEN  sls_sales/NULLIF(sls_quantity,0)
       ELSE  sls_price
    END AS  sls_price
    
FROM bronze.crm_sales_details;


SELECT * FROM silver.crm_sales_details ;



--bronze.erp_cust_az12
SELECT 

cid,
bdate,
gen FROM bronze.erp_cust_az12;


--correction IN cid 
SELECT 

CASE 
WHEN  cid like 'NAS%' THEN  substrINg(cid,4,len(cid)) 
ELSE  cid 
END AS  cid ,
bdate,
gen FROM bronze.erp_cust_az12;

--birth date(verifyINg the bdate if the date IN future or very old(>100yrs))
SELECT 
bdate FROM bronze.erp_cust_az12
 WHERE bdate < '1924-01-01' and bdate>getdate();

--cleanINg gen data 
SELECT DISTINCT gen  FROM bronze.erp_cust_az12;

SELECT DISTINCT gen  FROM (SELECT 
CASE 
WHEN  UPPER(TRIM(gen)) IN ('F','FEMALE') THEN  'Female'
 WHEN  UPPER(TRIM(gen)) IN ('M','MALE') THEN  'Male'
 WHEN  gen='' THEN  null
ELSE  gen
END AS  gen

FROM bronze.erp_cust_az12) AS t ;

---fINal query for erp_cust
SELECT 
CASE 
WHEN  cid like 'NAS%' THEN  substrINg(cid,4,len(cid)) 
ELSE  cid 
END AS  cid ,

CASE 
WHEN  bdate < '1924-01-01' and bdate>getdate() THEN  null
ELSE  bdate
END AS  badte,

CASE 
WHEN  UPPER(TRIM(gen)) IN ('F','FEMALE') THEN  'Female'
 WHEN  UPPER(TRIM(gen)) IN ('M','MALE')  THEN  'Male'
 ELSE  'n/a' --short 
END AS  gen

FROM bronze.erp_cust_az12;


--INsertINg INto silver layer

INSERT  INTO silver.erp_cust_az12
(cid,bdate,gen)

SELECT 
CASE 
WHEN  cid like 'NAS%' THEN  substrINg(cid,4,len(cid)) 
ELSE  cid 
END AS  cid ,

CASE 
WHEN  bdate < '1924-01-01' and bdate>getdate() THEN  null
ELSE  bdate
END AS  badte,

CASE 
WHEN  UPPER(TRIM(gen)) IN ('F','FEMALE') THEN  'Female'
 WHEN  UPPER(TRIM(gen)) IN ('M','MALE')  THEN  'Male'
 ELSE  'n/a' --short 
END AS  gen

FROM bronze.erp_cust_az12;


SELECT * FROM silver.erp_cust_az12;

--erp_loc_a101
INSERT  INTO silver.erp_loc_a101
(cid,cntry)
SELECT 
REPLACE(cid,'-','') AS cid,
CASE 
        WHEN  TRIM(cntry) = 'DE' THEN  'Germany'
        WHEN  TRIM(cntry) IN ('US', 'USA') THEN  'United States'
        WHEN  TRIM(cntry) = '' OR cntry IS NULL THEN  'n/a'
        ELSE  TRIM(cntry)
    END AS  cntry
FROM bronze.erp_loc_a101

SELECT * FROM silver.erp_loc_a101;

--bronze.erp_px_cat_g1v2

SELECT 
id,
cat,
subcat,
maINtenance
FROM bronze.erp_px_cat_g1v2;


--removINg spaces 
SELECT *
FROM bronze.erp_px_cat_g1v2  WHERE 
cat!=TRIM(cat) or 
subcat != TRIM(subcat) or  
maINtenance != TRIM(maINtenance);  

SELECT DISTINCT cat FROM bronze.erp_px_cat_g1v2


--INsertINg to silver 

INSERT  INTO silver.erp_px_cat_g1v2
(
id,
cat,
subcat,
maINtenance
)
SELECT *
FROM bronze.erp_px_cat_g1v2;

SELECT * FROM silver.erp_px_cat_g1v2;



correct the case of the words in the code make error free and looking good !
