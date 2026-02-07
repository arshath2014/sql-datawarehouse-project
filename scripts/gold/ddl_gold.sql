/*
===============================================================================
DDL Script: Create Gold Layer Views
===============================================================================


Description:
This script creates the Gold layer views for the data warehouse. 
The Gold layer represents the final, analytics-ready star schema composed 
of dimension and fact views derived from the Silver layer.

The views apply:
- Deduplication logic
-_attach and enrichment joins
- Data standardization and fallback rules
- Surrogate key generation
- Business-friendly column naming

Objects Created:
- gold.dim_customers   → Customer dimension view
- gold.dim_products    → Product dimension view
- gold.fact_sales      → Sales fact view

Data Sources:
- silver.crm_cust_info
- silver.erp_cust_az12
- silver.erp_loc_a101
- silver.crm_prd_info
- silver.erp_px_cat_g1v2
- silver.crm_sales_details

Key Transformations:
- Gender fallback logic using CRM + ERP attributes
- Surrogate keys generated with ROW_NUMBER()
- Active products filtered (end_date IS NULL)
- Fact table linked to dimensions via business keys
- Left joins used to preserve fact records

Usage:
These Gold views are intended for:
- BI reporting
- Dashboarding
- Ad-hoc analytics queries
- Downstream semantic models

Notes:
- Ensure Silver layer tables are refreshed before running this script.
- Surrogate keys are view-generated and not persisted.
- Validate foreign key integrity using the included check queries.

===============================================================================
*/




USE datawarehouse;


--Removing duplicates 

SELECT cst_id ,count(*) as count_

from (
SELECT
    ci.cst_id,
    ci.cst_key,
    ci.cst_firstname,
    ci.cst_lastname,
    ci.cst_marital_status,
    ci.cst_gndr,
    ci.cst_create_date,
    ca.bdate,
    ca.gen,
    la.cntry
FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ca
    ON ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 la
    ON ci.cst_key = la.cid
 ) t  group by cst_id having  count(*)>2  ;
   


   SELECT
    ci.cst_id,
    ci.cst_key,
    ci.cst_firstname,
    ci.cst_lastname,
    ci.cst_marital_status,
    ci.cst_gndr,
    ci.cst_create_date,
    ca.bdate,
    ca.gen,
    la.cntry
FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ca
    ON ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 la
    ON ci.cst_key = la.cid;


SELECT  cst_gndr,COUNT(*) AS number FROM(
    SELECT
   
    ci.cst_gndr,
   
    ca.gen,
   CASE  WHEN  ci.cst_gndr !='n/a' THEN ci.cst_gndr
   ELSE COALESCE(ca.gen,'n/a')
   END as gender
FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ca
    ON ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 la
    ON ci.cst_key = la.cid
) AS t  GROUP BY cst_gndr;


---Customers

CREATE VIEW gold.dim_customers as
SELECT
    ROW_NUMBER() OVER(ORDER BY cst_id) as customer_key,--<----surrogate key
    ci.cst_id as customer_id,
    ci.cst_key as customer_number,
    ci.cst_firstname as first_name,
    ci.cst_lastname as last_name,
    la.cntry as country,
    ci.cst_marital_status as marital_status,
    CASE  
        WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr
        ELSE COALESCE(ca.gen, 'n/a')
    END as gender,
    ca.bdate as date_of_birth,
    ci.cst_create_date as create_date
FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ca
    ON ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 la
    ON ci.cst_key = la.cid;

SELECT * FROM gold.dim_customers;




----Product
CREATE VIEW gold.dim_products AS
SELECT
    ROW_NUMBER() OVER(ORDER BY pn.prd_start_dt,pn.pr_key) as product_key, --<----surrogate key
    pn.prd_id as product_id,
    pn.pr_key as product_number,
    pn.prd_nm as product_name,
    pn.cat_id as category_id,
    en.cat as category,
    en.subcat as subcategory,
    en.maintenance ,
    pn.prd_cost as product_cost,
    pn.prd_line as product_line,
    pn.prd_start_dt as start_date
FROM silver.crm_prd_info pn
LEFT JOIN silver.erp_px_cat_g1v2 en ON pn.cat_id=en.id
WHERE pn.prd_end_dt is null;


SELECT * FROM gold.dim_products;


--Sales


SELECT
    sd.sls_ord_num,
    sd.sls_prd_key,
    sd.sls_cust_id,
    sd.sls_order_dt,
    sd.sls_ship_dt,
    sd.sls_due_dt,
    sd.sls_sales,
    sd.sls_quantity,
    sd.sls_price
FROM silver.crm_sales_details sd;
SELECT * FROM gold.dim_products;


CREATE VIEW gold.fact_sales AS
SELECT
    sd.sls_ord_num AS order_number,
    pr.product_key,--<----using surrogate key
    cu.customer_key,--<----using surrogate key
    sd.sls_order_dt AS order_date,
    sd.sls_ship_dt AS shipping_date,
    sd.sls_due_dt AS due_date,
    sd.sls_sales AS sales_amount,
    sd.sls_quantity AS quantity,
    sd.sls_price AS price
FROM silver.crm_sales_details sd
LEFT JOIN gold.dim_products pr
    ON sd.sls_prd_key = pr.product_number
LEFT JOIN gold.dim_customers cu
    ON sd.sls_cust_id = cu.customer_id;


SELECT * FROM gold.fact_sales;


--Foregin key integrity


SELECT *  FROM gold.fact_sales f 
LEFT JOIN gold.dim_customers c ON c.customer_key=f.customer_key
LEFT JOIN gold.dim_products p ON p.product_key=f.product_key 
WHERE p.product_key IS NULL ;



SELECT * FROM gold.dim_products;


SELECT * FROM gold.dim_customers;

SELECT * FROM gold.fact_sales;
